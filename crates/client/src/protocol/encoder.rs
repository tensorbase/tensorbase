use byteorder::WriteBytesExt;
use std::io::{Result, Write};
use uuid::Uuid;

pub trait Encoder {
    fn encode(&self, writer: &mut dyn Write) -> Result<()>;
}

impl Encoder for u64 {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        //let mut i = 0;
        let mut mx = *self;

        while mx >= 0x80 {
            writer.write_u8(mx as u8 | 0x80)?;
            mx >>= 7;
            //i += 1;
        }

        writer.write_u8(mx as u8)?;
        Ok(())
    }
}

impl Encoder for u8 {
    #[inline]
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_u8(*self)?;
        Ok(())
    }
}

impl Encoder for &[u8] {
    #[inline]
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_all(*self)?;
        Ok(())
    }
}

impl Encoder for &str {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let l: u64 = self.len() as u64;
        l.encode(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl Encoder for String {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let l: u64 = self.len() as u64;
        l.encode(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

/// Empty string or  0u8 byte
impl Encoder for () {
    #[inline]
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_u8(0)?;
        Ok(())
    }
}

impl Encoder for Uuid {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        if self.is_nil() {
            writer.write_u8(0)?;
            return Ok(());
        }

        let bytes = self.as_bytes();
        debug_assert!(bytes.len() == 16);
        16u8.encode(writer)?;
        writer.write_all(bytes)?;
        Ok(())
    }
}

pub struct StringEncoderAdapter<W: Write + Sized> {
    buf: Vec<u8>,
    inner: W,
}

impl<W: Write + Sized> StringEncoderAdapter<W> {
    pub fn new(writer: W) -> StringEncoderAdapter<W> {
        StringEncoderAdapter {
            buf: Vec::new(),
            inner: writer,
        }
    }
}

impl<W: Write + Sized> Write for StringEncoderAdapter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        let l: u64 = self.buf.len() as u64;
        l.encode(&mut self.inner)?;
        self.inner.write_all(&self.buf[..])?;

        self.buf.clear();
        Ok(())
    }
}

impl<W: Write + Sized> Drop for StringEncoderAdapter<W> {
    fn drop(&mut self) {
        self.flush().expect("TODO")
    }
}
