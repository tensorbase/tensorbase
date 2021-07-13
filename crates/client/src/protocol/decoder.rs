use crate::errors::{ConversionError, DriverError, Result};
use core::marker::PhantomData;
use futures::ready;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt::Debug, future::Future};
use tokio::io::ReadBuf;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

/// Read string data encoded as VarInt(length) + bytearray
pub(crate) struct ReadVString<'a, T: FromBytes, R> {
    length_: usize,
    data: Vec<u8>,
    inner: Pin<&'a mut R>,
    _marker: PhantomData<&'a T>,
}

pub trait FromBytes: Sized {
    fn from_bytes(bytes: &mut Vec<u8>) -> Result<Self>;
}

impl FromBytes for String {
    #[inline]
    fn from_bytes(bytes: &mut Vec<u8>) -> Result<Self> {
        let b = std::mem::take(bytes);
        String::from_utf8(b).map_err(|_e| ConversionError::Utf8.into())
    }
}

impl FromBytes for Vec<u8> {
    #[inline]
    fn from_bytes(bytes: &mut Vec<u8>) -> Result<Self> {
        Ok(std::mem::take(bytes))
    }
}

impl<'a, T: FromBytes, R: AsyncRead> ReadVString<'a, T, R> {
    pub(crate) fn new(reader: &'a mut R, length: usize) -> ReadVString<'a, T, R> {
        let data = unsafe {
            let mut v = Vec::with_capacity(length);
            v.set_len(length);
            v
        };
        let inner = unsafe { Pin::new_unchecked(reader) };
        ReadVString {
            length_: 0,
            data,
            inner,
            _marker: PhantomData,
        }
    }

    fn poll_get(&mut self, cx: &mut Context<'_>) -> Poll<Result<T>>
    where
        T: Debug,
    {
        loop {
            // log::info!("self.length_: {}", self.length_);
            if self.length_ == self.data.len() {
                let rt: Poll<Result<T>> = FromBytes::from_bytes(&mut self.data).into();
                // log::info!("{:?}", rt);
                return rt;
            } else {
                let mut read_buf = ReadBuf::new(&mut self.data[self.length_..]);
                ready!(self.inner.as_mut().poll_read(cx, &mut read_buf)?);
                self.length_ += read_buf.filled().len();
            }
        }
    }
}

impl<'a, T: FromBytes + std::fmt::Debug, R: AsyncRead> Future for ReadVString<'a, T, R> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.poll_get(cx)
    }
}
/// Read VarInt
pub(crate) struct ReadVInt<'a, R> {
    value: u64,
    i: u8,
    inner: Pin<&'a mut R>,
}

impl<'a, R: AsyncRead> ReadVInt<'a, R> {
    fn new(reader: &'a mut R) -> ReadVInt<'a, R> {
        let inner = unsafe { Pin::new_unchecked(reader) };
        ReadVInt {
            value: 0,
            i: 0,
            inner,
        }
    }

    fn poll_get(&mut self, cx: &mut Context<'_>) -> Poll<Result<u64>> {
        let mut b = [0u8; 1];
        loop {
            //let inner: Pin<&mut R> =  unsafe{ Pin::new_unchecked(self.inner) };
            let mut read_buf = ReadBuf::new(&mut b);
            ready!(self.inner.as_mut().poll_read(cx, &mut read_buf)?);

            if 0 == read_buf.filled().len() {
                return Poll::Ready(Err(DriverError::BrokenData.into()));
            }
            let b = b[0];

            self.value |= ((b & 0x7f) as u64) << (self.i);
            self.i += 7;

            if b < 0x80 {
                return Poll::Ready(Ok(self.value));
            };

            if self.i > 63 {
                return Poll::Ready(Err(DriverError::BrokenData.into()));
            };
        }
    }
}

impl<'a, R: AsyncRead> Future for ReadVInt<'a, R> {
    type Output = Result<u64>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.poll_get(cx)
    }
}

pub struct ValueReader<R> {
    inner: R,
}

impl<R: AsyncRead> ValueReader<R> {
    pub(super) fn new(reader: R) -> ValueReader<R> {
        ValueReader { inner: reader }
    }
    //TODO: Optimize reading note that reader is buffered data
    pub(super) fn read_vint(&mut self) -> ReadVInt<'_, R> {
        ReadVInt::new(&mut self.inner)
    }
    //TODO:  Optimize reading note that reader is buffered data
    pub(super) fn read_string<T: FromBytes>(
        &mut self,
        len: u64,
    ) -> ReadVString<'_, T, R> {
        ReadVString::new(&mut self.inner, len as usize)
    }

    #[inline]
    pub(super) fn as_mut(&mut self) -> &mut R {
        &mut self.inner
    }
}

pub(crate) struct Skip<'a, R> {
    value: usize,
    inner: Pin<&'a mut R>,
}

impl<'a, R: AsyncBufRead> Skip<'a, R> {
    pub(super) fn poll_skip(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        while self.value > 0 {
            let buf = ready!(self.inner.as_mut().poll_fill_buf(cx)?);
            let n = std::cmp::min(self.value, buf.len());
            self.inner.as_mut().consume(n);
            self.value -= n;
        }
        Ok(()).into()
    }
}

impl<'a, R: AsyncBufRead> Future for Skip<'a, R> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.poll_skip(cx)
    }
}

impl<R: AsyncBufRead + Unpin> ValueReader<R> {
    pub(super) fn skip(&mut self, len: u64) -> Skip<'_, R> {
        Skip {
            value: len as usize,
            inner: Pin::new(&mut self.inner),
        }
    }

    pub(super) async fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.inner.read_exact(&mut buf[..]).await?;

        Ok(buf[0])
    }
}
