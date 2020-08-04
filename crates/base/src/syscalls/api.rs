use super::{syscall, syscall_nr};

#[inline(always)]
pub unsafe fn read(fd: usize, buf: usize, count: usize) -> Result<usize, isize> {
    syscall(0, &[fd, buf, count])
}

#[inline(always)]
pub unsafe fn write(fd: usize, buf: usize, count: usize) -> Result<usize, isize> {
    syscall(1, &[fd, buf, count])
}

#[inline(always)]
pub unsafe fn open(filename: usize, flags: usize, mode: usize) -> Result<usize, isize> {
    syscall(2, &[filename, flags, mode])
}

#[inline(always)]
pub unsafe fn close(fd: usize) -> Result<usize, isize> {
    syscall(3, &[fd])
}

#[inline(always)]
pub unsafe fn stat(filename: usize, statbuf: usize) -> Result<usize, isize> {
    syscall(4, &[filename, statbuf])
}

#[inline(always)]
pub unsafe fn fstat(fd: usize, statbuf: usize) -> Result<usize, isize> {
    syscall(5, &[fd, statbuf])
}

#[inline(always)]
pub unsafe fn lstat(filename: usize, statbuf: usize) -> Result<usize, isize> {
    syscall(6, &[filename, statbuf])
}

#[inline(always)]
pub unsafe fn poll(ufds: usize, nfds: usize, timeout_msecs: usize) -> Result<usize, isize> {
    syscall(7, &[ufds, nfds, timeout_msecs])
}

#[inline(always)]
pub unsafe fn lseek(fd: usize, offset: usize, whence: usize) -> Result<usize, isize> {
    syscall(8, &[fd, offset, whence])
}

#[inline(always)]
pub unsafe fn mmap(
    addr: usize,
    len: usize,
    prot: usize,
    flags: usize,
    fd: usize,
    off: usize,
) -> Result<usize, isize> {
    syscall(9, &[addr, len, prot, flags, fd, off])
}

#[inline(always)]
pub unsafe fn mprotect(start: usize, len: usize, prot: usize) -> Result<usize, isize> {
    syscall(10, &[start, len, prot])
}

#[inline(always)]
pub unsafe fn munmap(addr: usize, len: usize) -> Result<usize, isize> {
    syscall(11, &[addr, len])
}

#[inline(always)]
pub unsafe fn brk(brk: usize) -> Result<usize, isize> {
    syscall(12, &[brk])
}

#[inline(always)]
pub unsafe fn rt_sigaction(
    sig: usize,
    act: usize,
    oact: usize,
    sigsetsize: usize,
) -> Result<usize, isize> {
    syscall(13, &[sig, act, oact, sigsetsize])
}

#[inline(always)]
pub unsafe fn rt_sigprocmask(
    how: usize,
    nset: usize,
    oset: usize,
    sigsetsize: usize,
) -> Result<usize, isize> {
    syscall(14, &[how, nset, oset, sigsetsize])
}

#[inline(always)]
pub unsafe fn rt_sigreturn() -> ! {
    syscall_nr(15, &[])
}

#[inline(always)]
pub unsafe fn ioctl(fd: usize, cmd: usize, arg: usize) -> Result<usize, isize> {
    syscall(16, &[fd, cmd, arg])
}

#[inline(always)]
pub unsafe fn pread64(
    fd: usize,
    buf: usize,
    count: usize,
    pos: usize,
) -> Result<usize, isize> {
    syscall(17, &[fd, buf, count, pos])
}

#[inline(always)]
pub unsafe fn pwrite64(
    fd: usize,
    buf: usize,
    count: usize,
    pos: usize,
) -> Result<usize, isize> {
    syscall(18, &[fd, buf, count, pos])
}

#[inline(always)]
pub unsafe fn readv(fd: usize, vec: usize, vlen: usize) -> Result<usize, isize> {
    syscall(19, &[fd, vec, vlen])
}

#[inline(always)]
pub unsafe fn writev(fd: usize, vec: usize, vlen: usize) -> Result<usize, isize> {
    syscall(20, &[fd, vec, vlen])
}

#[inline(always)]
pub unsafe fn access(filename: usize, mode: usize) -> Result<usize, isize> {
    syscall(21, &[filename, mode])
}

#[inline(always)]
pub unsafe fn pipe(fildes: usize) -> Result<usize, isize> {
    syscall(22, &[fildes])
}

#[inline(always)]
pub unsafe fn select(
    n: usize,
    inp: usize,
    outp: usize,
    exp: usize,
    tvp: usize,
) -> Result<usize, isize> {
    syscall(23, &[n, inp, outp, exp, tvp])
}

#[inline(always)]
pub unsafe fn sched_yield() -> Result<usize, isize> {
    syscall(24, &[])
}

#[inline(always)]
pub unsafe fn mremap(
    addr: usize,
    old_len: usize,
    new_len: usize,
    flags: usize,
    new_addr: usize,
) -> Result<usize, isize> {
    syscall(25, &[addr, old_len, new_len, flags, new_addr])
}

#[inline(always)]
pub unsafe fn msync(start: usize, len: usize, flags: usize) -> Result<usize, isize> {
    syscall(26, &[start, len, flags])
}

#[inline(always)]
pub unsafe fn mincore(start: usize, len: usize, vec: usize) -> Result<usize, isize> {
    syscall(27, &[start, len, vec])
}

#[inline(always)]
pub unsafe fn madvise(start: usize, len_in: usize, behavior: usize) -> Result<usize, isize> {
    syscall(28, &[start, len_in, behavior])
}

#[inline(always)]
pub unsafe fn shmget(key: usize, size: usize, shmflg: usize) -> Result<usize, isize> {
    syscall(29, &[key, size, shmflg])
}

#[inline(always)]
pub unsafe fn shmat(shmid: usize, shmaddr: usize, shmflg: usize) -> Result<usize, isize> {
    syscall(30, &[shmid, shmaddr, shmflg])
}

#[inline(always)]
pub unsafe fn shmctl(shmid: usize, cmd: usize, buf: usize) -> Result<usize, isize> {
    syscall(31, &[shmid, cmd, buf])
}

#[inline(always)]
pub unsafe fn dup(fildes: usize) -> Result<usize, isize> {
    syscall(32, &[fildes])
}

#[inline(always)]
pub unsafe fn dup2(oldfd: usize, newfd: usize) -> Result<usize, isize> {
    syscall(33, &[oldfd, newfd])
}

#[inline(always)]
pub unsafe fn pause() -> Result<usize, isize> {
    syscall(34, &[])
}

#[inline(always)]
pub unsafe fn nanosleep(rqtp: usize, rmtp: usize) -> Result<usize, isize> {
    syscall(35, &[rqtp, rmtp])
}

#[inline(always)]
pub unsafe fn getitimer(which: usize, value: usize) -> Result<usize, isize> {
    syscall(36, &[which, value])
}

#[inline(always)]
pub unsafe fn alarm(seconds: usize) -> Result<usize, isize> {
    syscall(37, &[seconds])
}

#[inline(always)]
pub unsafe fn setitimer(which: usize, value: usize, ovalue: usize) -> Result<usize, isize> {
    syscall(38, &[which, value, ovalue])
}

#[inline(always)]
pub unsafe fn getpid() -> Result<usize, isize> {
    syscall(39, &[])
}

#[inline(always)]
pub unsafe fn sendfile(
    out_fd: usize,
    in_fd: usize,
    offset: usize,
    count: usize,
) -> Result<usize, isize> {
    syscall(40, &[out_fd, in_fd, offset, count])
}

#[inline(always)]
pub unsafe fn socket(family: usize, r#type: usize, protocol: usize) -> Result<usize, isize> {
    syscall(41, &[family, r#type, protocol])
}

#[inline(always)]
pub unsafe fn connect(fd: usize, uservaddr: usize, addrlen: usize) -> Result<usize, isize> {
    syscall(42, &[fd, uservaddr, addrlen])
}

#[inline(always)]
pub unsafe fn accept(
    fd: usize,
    upeer_sockaddr: usize,
    upeer_addrlen: usize,
) -> Result<usize, isize> {
    syscall(43, &[fd, upeer_sockaddr, upeer_addrlen])
}

#[inline(always)]
pub unsafe fn sendto(
    fd: usize,
    buff: usize,
    len: usize,
    flags: usize,
    addr: usize,
    addr_len: usize,
) -> Result<usize, isize> {
    syscall(44, &[fd, buff, len, flags, addr, addr_len])
}

#[inline(always)]
pub unsafe fn recvfrom(
    fd: usize,
    ubuf: usize,
    size: usize,
    flags: usize,
    addr: usize,
    addr_len: usize,
) -> Result<usize, isize> {
    syscall(45, &[fd, ubuf, size, flags, addr, addr_len])
}

#[inline(always)]
pub unsafe fn sendmsg(fd: usize, msg: usize, flags: usize) -> Result<usize, isize> {
    syscall(46, &[fd, msg, flags])
}

#[inline(always)]
pub unsafe fn recvmsg(fd: usize, msg: usize, flags: usize) -> Result<usize, isize> {
    syscall(47, &[fd, msg, flags])
}

#[inline(always)]
pub unsafe fn shutdown(fd: usize, how: usize) -> Result<usize, isize> {
    syscall(48, &[fd, how])
}

#[inline(always)]
pub unsafe fn bind(fd: usize, umyaddr: usize, addrlen: usize) -> Result<usize, isize> {
    syscall(49, &[fd, umyaddr, addrlen])
}

#[inline(always)]
pub unsafe fn listen(fd: usize, backlog: usize) -> Result<usize, isize> {
    syscall(50, &[fd, backlog])
}

#[inline(always)]
pub unsafe fn getsockname(
    fd: usize,
    usockaddr: usize,
    usockaddr_len: usize,
) -> Result<usize, isize> {
    syscall(51, &[fd, usockaddr, usockaddr_len])
}

#[inline(always)]
pub unsafe fn getpeername(
    fd: usize,
    usockaddr: usize,
    usockaddr_len: usize,
) -> Result<usize, isize> {
    syscall(52, &[fd, usockaddr, usockaddr_len])
}

#[inline(always)]
pub unsafe fn socketpair(
    family: usize,
    r#type: usize,
    protocol: usize,
    usockvec: usize,
) -> Result<usize, isize> {
    syscall(53, &[family, r#type, protocol, usockvec])
}

#[inline(always)]
pub unsafe fn setsockopt(
    fd: usize,
    level: usize,
    optname: usize,
    optval: usize,
    optlen: usize,
) -> Result<usize, isize> {
    syscall(54, &[fd, level, optname, optval, optlen])
}

#[inline(always)]
pub unsafe fn getsockopt(
    fd: usize,
    level: usize,
    optname: usize,
    optval: usize,
    optlen: usize,
) -> Result<usize, isize> {
    syscall(55, &[fd, level, optname, optval, optlen])
}

#[inline(always)]
pub unsafe fn clone(
    clone_flags: usize,
    newsp: usize,
    parent_tidptr: usize,
    child_tidptr: usize,
    tls: usize,
) -> Result<usize, isize> {
    syscall(56, &[clone_flags, newsp, parent_tidptr, child_tidptr, tls])
}

#[inline(always)]
pub unsafe fn fork() -> Result<usize, isize> {
    syscall(57, &[])
}

#[inline(always)]
pub unsafe fn vfork() -> Result<usize, isize> {
    syscall(58, &[])
}

#[inline(always)]
pub unsafe fn execve(filename: usize, argv: usize, envp: usize) -> Result<usize, isize> {
    syscall(59, &[filename, argv, envp])
}

#[inline(always)]
pub unsafe fn exit(error_code: usize) -> ! {
    syscall_nr(60, &[error_code])
}

#[inline(always)]
pub unsafe fn wait4(
    upid: usize,
    stat_addr: usize,
    options: usize,
    ru: usize,
) -> Result<usize, isize> {
    syscall(61, &[upid, stat_addr, options, ru])
}

#[inline(always)]
pub unsafe fn kill(pid: usize, sig: usize) -> Result<usize, isize> {
    syscall(62, &[pid, sig])
}

#[inline(always)]
pub unsafe fn uname(name: usize) -> Result<usize, isize> {
    syscall(63, &[name])
}

#[inline(always)]
pub unsafe fn semget(key: usize, nsems: usize, semflg: usize) -> Result<usize, isize> {
    syscall(64, &[key, nsems, semflg])
}

#[inline(always)]
pub unsafe fn semop(semid: usize, tsops: usize, nsops: usize) -> Result<usize, isize> {
    syscall(65, &[semid, tsops, nsops])
}

#[inline(always)]
pub unsafe fn semctl(
    semid: usize,
    semnum: usize,
    cmd: usize,
    arg: usize,
) -> Result<usize, isize> {
    syscall(66, &[semid, semnum, cmd, arg])
}

#[inline(always)]
pub unsafe fn shmdt(shmaddr: usize) -> Result<usize, isize> {
    syscall(67, &[shmaddr])
}

#[inline(always)]
pub unsafe fn msgget(key: usize, msgflg: usize) -> Result<usize, isize> {
    syscall(68, &[key, msgflg])
}

#[inline(always)]
pub unsafe fn msgsnd(
    msqid: usize,
    msgp: usize,
    msgsz: usize,
    msgflg: usize,
) -> Result<usize, isize> {
    syscall(69, &[msqid, msgp, msgsz, msgflg])
}

#[inline(always)]
pub unsafe fn msgrcv(
    msqid: usize,
    msgp: usize,
    msgsz: usize,
    msgtyp: usize,
    msgflg: usize,
) -> Result<usize, isize> {
    syscall(70, &[msqid, msgp, msgsz, msgtyp, msgflg])
}

#[inline(always)]
pub unsafe fn msgctl(msqid: usize, cmd: usize, buf: usize) -> Result<usize, isize> {
    syscall(71, &[msqid, cmd, buf])
}

#[inline(always)]
pub unsafe fn fcntl(fd: usize, cmd: usize, arg: usize) -> Result<usize, isize> {
    syscall(72, &[fd, cmd, arg])
}

#[inline(always)]
pub unsafe fn flock(fd: usize, cmd: usize) -> Result<usize, isize> {
    syscall(73, &[fd, cmd])
}

#[inline(always)]
pub unsafe fn fsync(fd: usize) -> Result<usize, isize> {
    syscall(74, &[fd])
}

#[inline(always)]
pub unsafe fn fdatasync(fd: usize) -> Result<usize, isize> {
    syscall(75, &[fd])
}

#[inline(always)]
pub unsafe fn truncate(path: usize, length: usize) -> Result<usize, isize> {
    syscall(76, &[path, length])
}

#[inline(always)]
pub unsafe fn ftruncate(fd: usize, length: usize) -> Result<usize, isize> {
    syscall(77, &[fd, length])
}

#[inline(always)]
pub unsafe fn getdents(fd: usize, dirent: usize, count: usize) -> Result<usize, isize> {
    syscall(78, &[fd, dirent, count])
}

#[inline(always)]
pub unsafe fn getcwd(buf: usize, size: usize) -> Result<usize, isize> {
    syscall(79, &[buf, size])
}

#[inline(always)]
pub unsafe fn chdir(filename: usize) -> Result<usize, isize> {
    syscall(80, &[filename])
}

#[inline(always)]
pub unsafe fn fchdir(fd: usize) -> Result<usize, isize> {
    syscall(81, &[fd])
}

#[inline(always)]
pub unsafe fn rename(oldname: usize, newname: usize) -> Result<usize, isize> {
    syscall(82, &[oldname, newname])
}

#[inline(always)]
pub unsafe fn mkdir(pathname: usize, mode: usize) -> Result<usize, isize> {
    syscall(83, &[pathname, mode])
}

#[inline(always)]
pub unsafe fn rmdir(pathname: usize) -> Result<usize, isize> {
    syscall(84, &[pathname])
}

#[inline(always)]
pub unsafe fn creat(pathname: usize, mode: usize) -> Result<usize, isize> {
    syscall(85, &[pathname, mode])
}

#[inline(always)]
pub unsafe fn link(oldname: usize, newname: usize) -> Result<usize, isize> {
    syscall(86, &[oldname, newname])
}

#[inline(always)]
pub unsafe fn unlink(pathname: usize) -> Result<usize, isize> {
    syscall(87, &[pathname])
}

#[inline(always)]
pub unsafe fn symlink(oldname: usize, newname: usize) -> Result<usize, isize> {
    syscall(88, &[oldname, newname])
}

#[inline(always)]
pub unsafe fn readlink(path: usize, buf: usize, bufsiz: usize) -> Result<usize, isize> {
    syscall(89, &[path, buf, bufsiz])
}

#[inline(always)]
pub unsafe fn chmod(filename: usize, mode: usize) -> Result<usize, isize> {
    syscall(90, &[filename, mode])
}

#[inline(always)]
pub unsafe fn fchmod(fd: usize, mode: usize) -> Result<usize, isize> {
    syscall(91, &[fd, mode])
}

#[inline(always)]
pub unsafe fn chown(filename: usize, user: usize, group: usize) -> Result<usize, isize> {
    syscall(92, &[filename, user, group])
}

#[inline(always)]
pub unsafe fn fchown(fd: usize, user: usize, group: usize) -> Result<usize, isize> {
    syscall(93, &[fd, user, group])
}

#[inline(always)]
pub unsafe fn lchown(filename: usize, user: usize, group: usize) -> Result<usize, isize> {
    syscall(94, &[filename, user, group])
}

#[inline(always)]
pub unsafe fn umask(mask: usize) -> Result<usize, isize> {
    syscall(95, &[mask])
}

#[inline(always)]
pub unsafe fn gettimeofday(tv: usize, tz: usize) -> Result<usize, isize> {
    syscall(96, &[tv, tz])
}

#[inline(always)]
pub unsafe fn getrlimit(resource: usize, rlim: usize) -> Result<usize, isize> {
    syscall(97, &[resource, rlim])
}

#[inline(always)]
pub unsafe fn getrusage(who: usize, ru: usize) -> Result<usize, isize> {
    syscall(98, &[who, ru])
}

#[inline(always)]
pub unsafe fn sysinfo(info: usize) -> Result<usize, isize> {
    syscall(99, &[info])
}

#[inline(always)]
pub unsafe fn times(tbuf: usize) -> Result<usize, isize> {
    syscall(100, &[tbuf])
}

#[inline(always)]
pub unsafe fn ptrace(
    request: usize,
    pid: usize,
    addr: usize,
    data: usize,
) -> Result<usize, isize> {
    syscall(101, &[request, pid, addr, data])
}

#[inline(always)]
pub unsafe fn getuid() -> Result<usize, isize> {
    syscall(102, &[])
}

#[inline(always)]
pub unsafe fn syslog(r#type: usize, buf: usize, len: usize) -> Result<usize, isize> {
    syscall(103, &[r#type, buf, len])
}

#[inline(always)]
pub unsafe fn getgid() -> Result<usize, isize> {
    syscall(104, &[])
}

#[inline(always)]
pub unsafe fn setuid(uid: usize) -> Result<usize, isize> {
    syscall(105, &[uid])
}

#[inline(always)]
pub unsafe fn setgid(gid: usize) -> Result<usize, isize> {
    syscall(106, &[gid])
}

#[inline(always)]
pub unsafe fn geteuid() -> Result<usize, isize> {
    syscall(107, &[])
}

#[inline(always)]
pub unsafe fn getegid() -> Result<usize, isize> {
    syscall(108, &[])
}

#[inline(always)]
pub unsafe fn setpgid(pid: usize, pgid: usize) -> Result<usize, isize> {
    syscall(109, &[pid, pgid])
}

#[inline(always)]
pub unsafe fn getppid() -> Result<usize, isize> {
    syscall(110, &[])
}

#[inline(always)]
pub unsafe fn getpgrp() -> Result<usize, isize> {
    syscall(111, &[])
}

#[inline(always)]
pub unsafe fn setsid() -> Result<usize, isize> {
    syscall(112, &[])
}

#[inline(always)]
pub unsafe fn setreuid(ruid: usize, euid: usize) -> Result<usize, isize> {
    syscall(113, &[ruid, euid])
}

#[inline(always)]
pub unsafe fn setregid(rgid: usize, egid: usize) -> Result<usize, isize> {
    syscall(114, &[rgid, egid])
}

#[inline(always)]
pub unsafe fn getgroups(gidsetsize: usize, grouplist: usize) -> Result<usize, isize> {
    syscall(115, &[gidsetsize, grouplist])
}

#[inline(always)]
pub unsafe fn setgroups(gidsetsize: usize, grouplist: usize) -> Result<usize, isize> {
    syscall(116, &[gidsetsize, grouplist])
}

#[inline(always)]
pub unsafe fn setresuid(ruid: usize, euid: usize, suid: usize) -> Result<usize, isize> {
    syscall(117, &[ruid, euid, suid])
}

#[inline(always)]
pub unsafe fn getresuid(ruidp: usize, euidp: usize, suidp: usize) -> Result<usize, isize> {
    syscall(118, &[ruidp, euidp, suidp])
}

#[inline(always)]
pub unsafe fn setresgid(rgid: usize, egid: usize, sgid: usize) -> Result<usize, isize> {
    syscall(119, &[rgid, egid, sgid])
}

#[inline(always)]
pub unsafe fn getresgid(rgidp: usize, egidp: usize, sgidp: usize) -> Result<usize, isize> {
    syscall(120, &[rgidp, egidp, sgidp])
}

#[inline(always)]
pub unsafe fn getpgid(pid: usize) -> Result<usize, isize> {
    syscall(121, &[pid])
}

#[inline(always)]
pub unsafe fn setfsuid(uid: usize) -> Result<usize, isize> {
    syscall(122, &[uid])
}

#[inline(always)]
pub unsafe fn setfsgid(gid: usize) -> Result<usize, isize> {
    syscall(123, &[gid])
}

#[inline(always)]
pub unsafe fn getsid(pid: usize) -> Result<usize, isize> {
    syscall(124, &[pid])
}

#[inline(always)]
pub unsafe fn capget(header: usize, dataptr: usize) -> Result<usize, isize> {
    syscall(125, &[header, dataptr])
}

#[inline(always)]
pub unsafe fn capset(header: usize, data: usize) -> Result<usize, isize> {
    syscall(126, &[header, data])
}

#[inline(always)]
pub unsafe fn rt_sigpending(uset: usize, sigsetsize: usize) -> Result<usize, isize> {
    syscall(127, &[uset, sigsetsize])
}

#[inline(always)]
pub unsafe fn rt_sigtimedwait(
    uthese: usize,
    uinfo: usize,
    uts: usize,
    sigsetsize: usize,
) -> Result<usize, isize> {
    syscall(128, &[uthese, uinfo, uts, sigsetsize])
}

#[inline(always)]
pub unsafe fn rt_sigqueueinfo(pid: usize, sig: usize, uinfo: usize) -> Result<usize, isize> {
    syscall(129, &[pid, sig, uinfo])
}

#[inline(always)]
pub unsafe fn rt_sigsuspend(unewset: usize, sigsetsize: usize) -> Result<usize, isize> {
    syscall(130, &[unewset, sigsetsize])
}

#[inline(always)]
pub unsafe fn sigaltstack(uss: usize, uoss: usize) -> Result<usize, isize> {
    syscall(131, &[uss, uoss])
}

#[inline(always)]
pub unsafe fn utime(filename: usize, times: usize) -> Result<usize, isize> {
    syscall(132, &[filename, times])
}

#[inline(always)]
pub unsafe fn mknod(filename: usize, mode: usize, dev: usize) -> Result<usize, isize> {
    syscall(133, &[filename, mode, dev])
}

#[inline(always)]
pub unsafe fn uselib(library: usize) -> Result<usize, isize> {
    syscall(134, &[library])
}

#[inline(always)]
pub unsafe fn personality(personality: usize) -> Result<usize, isize> {
    syscall(135, &[personality])
}

#[inline(always)]
pub unsafe fn ustat(dev: usize, ubuf: usize) -> Result<usize, isize> {
    syscall(136, &[dev, ubuf])
}

#[inline(always)]
pub unsafe fn statfs(pathname: usize, buf: usize) -> Result<usize, isize> {
    syscall(137, &[pathname, buf])
}

#[inline(always)]
pub unsafe fn fstatfs(fd: usize, buf: usize) -> Result<usize, isize> {
    syscall(138, &[fd, buf])
}

#[inline(always)]
pub unsafe fn sysfs(option: usize, arg1: usize, arg2: usize) -> Result<usize, isize> {
    syscall(139, &[option, arg1, arg2])
}

#[inline(always)]
pub unsafe fn getpriority(which: usize, who: usize) -> Result<usize, isize> {
    syscall(140, &[which, who])
}

#[inline(always)]
pub unsafe fn setpriority(which: usize, who: usize, niceval: usize) -> Result<usize, isize> {
    syscall(141, &[which, who, niceval])
}

#[inline(always)]
pub unsafe fn sched_setparam(pid: usize, param: usize) -> Result<usize, isize> {
    syscall(142, &[pid, param])
}

#[inline(always)]
pub unsafe fn sched_getparam(pid: usize, param: usize) -> Result<usize, isize> {
    syscall(143, &[pid, param])
}

#[inline(always)]
pub unsafe fn sched_setscheduler(
    pid: usize,
    policy: usize,
    param: usize,
) -> Result<usize, isize> {
    syscall(144, &[pid, policy, param])
}

#[inline(always)]
pub unsafe fn sched_getscheduler(pid: usize) -> Result<usize, isize> {
    syscall(145, &[pid])
}

#[inline(always)]
pub unsafe fn sched_get_priority_max(policy: usize) -> Result<usize, isize> {
    syscall(146, &[policy])
}

#[inline(always)]
pub unsafe fn sched_get_priority_min(policy: usize) -> Result<usize, isize> {
    syscall(147, &[policy])
}

#[inline(always)]
pub unsafe fn sched_rr_get_interval(pid: usize, interval: usize) -> Result<usize, isize> {
    syscall(148, &[pid, interval])
}

#[inline(always)]
pub unsafe fn mlock(start: usize, len: usize) -> Result<usize, isize> {
    syscall(149, &[start, len])
}

#[inline(always)]
pub unsafe fn munlock(start: usize, len: usize) -> Result<usize, isize> {
    syscall(150, &[start, len])
}

#[inline(always)]
pub unsafe fn mlockall(flags: usize) -> Result<usize, isize> {
    syscall(151, &[flags])
}

#[inline(always)]
pub unsafe fn munlockall() -> Result<usize, isize> {
    syscall(152, &[])
}

#[inline(always)]
pub unsafe fn vhangup() -> Result<usize, isize> {
    syscall(153, &[])
}

#[inline(always)]
pub unsafe fn modify_ldt(func: usize, ptr: usize, bytecount: usize) -> Result<usize, isize> {
    syscall(154, &[func, ptr, bytecount])
}

#[inline(always)]
pub unsafe fn pivot_root(new_root: usize, put_old: usize) -> Result<usize, isize> {
    syscall(155, &[new_root, put_old])
}

#[inline(always)]
pub unsafe fn _sysctl(args: usize) -> Result<usize, isize> {
    syscall(156, &[args])
}

#[inline(always)]
pub unsafe fn prctl(
    option: usize,
    arg2: usize,
    arg3: usize,
    arg4: usize,
    arg5: usize,
) -> Result<usize, isize> {
    syscall(157, &[option, arg2, arg3, arg4, arg5])
}

#[inline(always)]
pub unsafe fn arch_prctl(task: usize, code: usize, addr: usize) -> Result<usize, isize> {
    syscall(158, &[task, code, addr])
}

#[inline(always)]
pub unsafe fn adjtimex(txc_p: usize) -> Result<usize, isize> {
    syscall(159, &[txc_p])
}

#[inline(always)]
pub unsafe fn setrlimit(resource: usize, rlim: usize) -> Result<usize, isize> {
    syscall(160, &[resource, rlim])
}

#[inline(always)]
pub unsafe fn chroot(filename: usize) -> Result<usize, isize> {
    syscall(161, &[filename])
}

#[inline(always)]
pub unsafe fn sync() -> Result<usize, isize> {
    syscall(162, &[])
}

#[inline(always)]
pub unsafe fn acct(name: usize) -> Result<usize, isize> {
    syscall(163, &[name])
}

#[inline(always)]
pub unsafe fn settimeofday(tv: usize, tz: usize) -> Result<usize, isize> {
    syscall(164, &[tv, tz])
}

#[inline(always)]
pub unsafe fn mount(
    dev_name: usize,
    dir_name: usize,
    r#type: usize,
    flags: usize,
    data: usize,
) -> Result<usize, isize> {
    syscall(165, &[dev_name, dir_name, r#type, flags, data])
}

#[inline(always)]
pub unsafe fn umount2(name: usize, flags: usize) -> Result<usize, isize> {
    syscall(166, &[name, flags])
}

#[inline(always)]
pub unsafe fn swapon(specialfile: usize, swap_flags: usize) -> Result<usize, isize> {
    syscall(167, &[specialfile, swap_flags])
}

#[inline(always)]
pub unsafe fn swapoff(specialfile: usize) -> Result<usize, isize> {
    syscall(168, &[specialfile])
}

#[inline(always)]
pub unsafe fn reboot(
    magic1: usize,
    magic2: usize,
    cmd: usize,
    arg: usize,
) -> Result<usize, isize> {
    syscall(169, &[magic1, magic2, cmd, arg])
}

#[inline(always)]
pub unsafe fn sethostname(name: usize, len: usize) -> Result<usize, isize> {
    syscall(170, &[name, len])
}

#[inline(always)]
pub unsafe fn setdomainname(name: usize, len: usize) -> Result<usize, isize> {
    syscall(171, &[name, len])
}

#[inline(always)]
pub unsafe fn iopl(level: usize) -> Result<usize, isize> {
    syscall(172, &[level])
}

#[inline(always)]
pub unsafe fn ioperm(from: usize, num: usize, turn_on: usize) -> Result<usize, isize> {
    syscall(173, &[from, num, turn_on])
}

#[inline(always)]
pub unsafe fn create_module(name: usize, size: usize) -> Result<usize, isize> {
    syscall(174, &[name, size])
}

#[inline(always)]
pub unsafe fn init_module(umod: usize, len: usize, uargs: usize) -> Result<usize, isize> {
    syscall(175, &[umod, len, uargs])
}

#[inline(always)]
pub unsafe fn delete_module(name_user: usize, flags: usize) -> Result<usize, isize> {
    syscall(176, &[name_user, flags])
}

#[inline(always)]
pub unsafe fn get_kernel_syms(table: usize) -> Result<usize, isize> {
    syscall(177, &[table])
}

#[inline(always)]
pub unsafe fn query_module(
    name: usize,
    which: usize,
    buf: usize,
    bufsize: usize,
    ret: usize,
) -> Result<usize, isize> {
    syscall(178, &[name, which, buf, bufsize, ret])
}

#[inline(always)]
pub unsafe fn quotactl(
    cmd: usize,
    special: usize,
    id: usize,
    addr: usize,
) -> Result<usize, isize> {
    syscall(179, &[cmd, special, id, addr])
}

#[inline(always)]
pub unsafe fn nfsservctl(cmd: usize, argp: usize, resp: usize) -> Result<usize, isize> {
    syscall(180, &[cmd, argp, resp])
}

#[inline(always)]
pub unsafe fn getpmsg(
    fd: usize,
    ctlptr: usize,
    dataptr: usize,
    bandp: usize,
    flagsp: usize,
) -> Result<usize, isize> {
    syscall(181, &[fd, ctlptr, dataptr, bandp, flagsp])
}

#[inline(always)]
pub unsafe fn putpmsg(
    fd: usize,
    ctlptr: usize,
    dataptr: usize,
    bandp: usize,
    flagsp: usize,
) -> Result<usize, isize> {
    syscall(182, &[fd, ctlptr, dataptr, bandp, flagsp])
}

#[inline(always)]
pub unsafe fn afs_syscall(
    param1: usize,
    param2: usize,
    param3: usize,
    param4: usize,
) -> Result<usize, isize> {
    syscall(183, &[param1, param2, param3, param4])
}

#[inline(always)]
pub unsafe fn tuxcall(action: usize, req: usize) -> Result<usize, isize> {
    syscall(184, &[action, req])
}

#[inline(always)]
pub unsafe fn security(id: usize, call: usize, args: usize) -> Result<usize, isize> {
    syscall(185, &[id, call, args])
}

#[inline(always)]
pub unsafe fn gettid() -> Result<usize, isize> {
    syscall(186, &[])
}

#[inline(always)]
pub unsafe fn readahead(fd: usize, offset: usize, count: usize) -> Result<usize, isize> {
    syscall(187, &[fd, offset, count])
}

#[inline(always)]
pub unsafe fn setxattr(
    pathname: usize,
    name: usize,
    value: usize,
    size: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(188, &[pathname, name, value, size, flags])
}

#[inline(always)]
pub unsafe fn lsetxattr(
    pathname: usize,
    name: usize,
    value: usize,
    size: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(189, &[pathname, name, value, size, flags])
}

#[inline(always)]
pub unsafe fn fsetxattr(
    fd: usize,
    name: usize,
    value: usize,
    size: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(190, &[fd, name, value, size, flags])
}

#[inline(always)]
pub unsafe fn getxattr(
    pathname: usize,
    name: usize,
    value: usize,
    size: usize,
) -> Result<usize, isize> {
    syscall(191, &[pathname, name, value, size])
}

#[inline(always)]
pub unsafe fn lgetxattr(
    pathname: usize,
    name: usize,
    value: usize,
    size: usize,
) -> Result<usize, isize> {
    syscall(192, &[pathname, name, value, size])
}

#[inline(always)]
pub unsafe fn fgetxattr(
    fd: usize,
    name: usize,
    value: usize,
    size: usize,
) -> Result<usize, isize> {
    syscall(193, &[fd, name, value, size])
}

#[inline(always)]
pub unsafe fn listxattr(pathname: usize, list: usize, size: usize) -> Result<usize, isize> {
    syscall(194, &[pathname, list, size])
}

#[inline(always)]
pub unsafe fn llistxattr(pathname: usize, list: usize, size: usize) -> Result<usize, isize> {
    syscall(195, &[pathname, list, size])
}

#[inline(always)]
pub unsafe fn flistxattr(fd: usize, list: usize, size: usize) -> Result<usize, isize> {
    syscall(196, &[fd, list, size])
}

#[inline(always)]
pub unsafe fn removexattr(pathname: usize, name: usize) -> Result<usize, isize> {
    syscall(197, &[pathname, name])
}

#[inline(always)]
pub unsafe fn lremovexattr(pathname: usize, name: usize) -> Result<usize, isize> {
    syscall(198, &[pathname, name])
}

#[inline(always)]
pub unsafe fn fremovexattr(fd: usize, name: usize) -> Result<usize, isize> {
    syscall(199, &[fd, name])
}

#[inline(always)]
pub unsafe fn tkill(pid: usize, sig: usize) -> Result<usize, isize> {
    syscall(200, &[pid, sig])
}

#[inline(always)]
pub unsafe fn time(tloc: usize) -> Result<usize, isize> {
    syscall(201, &[tloc])
}

#[inline(always)]
pub unsafe fn futex(
    uaddr: usize,
    op: usize,
    val: usize,
    utime: usize,
    uaddr2: usize,
    val3: usize,
) -> Result<usize, isize> {
    syscall(202, &[uaddr, op, val, utime, uaddr2, val3])
}

#[inline(always)]
pub unsafe fn sched_setaffinity(
    pid: usize,
    len: usize,
    user_mask_ptr: usize,
) -> Result<usize, isize> {
    syscall(203, &[pid, len, user_mask_ptr])
}

#[inline(always)]
pub unsafe fn sched_getaffinity(
    pid: usize,
    len: usize,
    user_mask_ptr: usize,
) -> Result<usize, isize> {
    syscall(204, &[pid, len, user_mask_ptr])
}

#[inline(always)]
pub unsafe fn set_thread_area(u_info: usize) -> Result<usize, isize> {
    syscall(205, &[u_info])
}

#[inline(always)]
pub unsafe fn io_setup(nr_events: usize, ctxp: usize) -> Result<usize, isize> {
    syscall(206, &[nr_events, ctxp])
}

#[inline(always)]
pub unsafe fn io_destroy(ctx: usize) -> Result<usize, isize> {
    syscall(207, &[ctx])
}

#[inline(always)]
pub unsafe fn io_getevents(
    ctx_id: usize,
    min_nr: usize,
    nr: usize,
    events: usize,
    timeout: usize,
) -> Result<usize, isize> {
    syscall(208, &[ctx_id, min_nr, nr, events, timeout])
}

#[inline(always)]
pub unsafe fn io_submit(ctx_id: usize, nr: usize, iocbpp: usize) -> Result<usize, isize> {
    syscall(209, &[ctx_id, nr, iocbpp])
}

#[inline(always)]
pub unsafe fn io_cancel(ctx_id: usize, iocb: usize, result: usize) -> Result<usize, isize> {
    syscall(210, &[ctx_id, iocb, result])
}

#[inline(always)]
pub unsafe fn get_thread_area(u_info: usize) -> Result<usize, isize> {
    syscall(211, &[u_info])
}

#[inline(always)]
pub unsafe fn lookup_dcookie(cookie64: usize, buf: usize, len: usize) -> Result<usize, isize> {
    syscall(212, &[cookie64, buf, len])
}

#[inline(always)]
pub unsafe fn epoll_create(size: usize) -> Result<usize, isize> {
    syscall(213, &[size])
}

#[inline(always)]
pub unsafe fn epoll_ctl_old(op: usize, fd: usize, event: usize) -> Result<usize, isize> {
    syscall(214, &[op, fd, event])
}

#[inline(always)]
pub unsafe fn epoll_wait_old(
    events: usize,
    maxevents: usize,
    timeout: usize,
) -> Result<usize, isize> {
    syscall(215, &[events, maxevents, timeout])
}

#[inline(always)]
pub unsafe fn remap_file_pages(
    start: usize,
    size: usize,
    prot: usize,
    pgoff: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(216, &[start, size, prot, pgoff, flags])
}

#[inline(always)]
pub unsafe fn getdents64(fd: usize, dirent: usize, count: usize) -> Result<usize, isize> {
    syscall(217, &[fd, dirent, count])
}

#[inline(always)]
pub unsafe fn set_tid_address(tidptr: usize) -> Result<usize, isize> {
    syscall(218, &[tidptr])
}

#[inline(always)]
pub unsafe fn restart_syscall() -> Result<usize, isize> {
    syscall(219, &[])
}

#[inline(always)]
pub unsafe fn semtimedop(
    semid: usize,
    tsops: usize,
    nsops: usize,
    timeout: usize,
) -> Result<usize, isize> {
    syscall(220, &[semid, tsops, nsops, timeout])
}

#[inline(always)]
pub unsafe fn fadvise64(
    fd: usize,
    offset: usize,
    len: usize,
    advice: usize,
) -> Result<usize, isize> {
    syscall(221, &[fd, offset, len, advice])
}

#[inline(always)]
pub unsafe fn timer_create(
    which_clock: usize,
    timer_event_spec: usize,
    created_timer_id: usize,
) -> Result<usize, isize> {
    syscall(222, &[which_clock, timer_event_spec, created_timer_id])
}

#[inline(always)]
pub unsafe fn timer_settime(
    timer_id: usize,
    flags: usize,
    new_setting: usize,
    old_setting: usize,
) -> Result<usize, isize> {
    syscall(223, &[timer_id, flags, new_setting, old_setting])
}

#[inline(always)]
pub unsafe fn timer_gettime(timer_id: usize, setting: usize) -> Result<usize, isize> {
    syscall(224, &[timer_id, setting])
}

#[inline(always)]
pub unsafe fn timer_getoverrun(timer_id: usize) -> Result<usize, isize> {
    syscall(225, &[timer_id])
}

#[inline(always)]
pub unsafe fn timer_delete(timer_id: usize) -> Result<usize, isize> {
    syscall(226, &[timer_id])
}

#[inline(always)]
pub unsafe fn clock_settime(which_clock: usize, tp: usize) -> Result<usize, isize> {
    syscall(227, &[which_clock, tp])
}

#[inline(always)]
pub unsafe fn clock_gettime(which_clock: usize, tp: usize) -> Result<usize, isize> {
    syscall(228, &[which_clock, tp])
}

#[inline(always)]
pub unsafe fn clock_getres(which_clock: usize, tp: usize) -> Result<usize, isize> {
    syscall(229, &[which_clock, tp])
}

#[inline(always)]
pub unsafe fn clock_nanosleep(
    which_clock: usize,
    flags: usize,
    rqtp: usize,
    rmtp: usize,
) -> Result<usize, isize> {
    syscall(230, &[which_clock, flags, rqtp, rmtp])
}

#[inline(always)]
pub unsafe fn exit_group(error_code: usize) -> ! {
    syscall_nr(231, &[error_code])
}

#[inline(always)]
pub unsafe fn epoll_wait(
    epfd: usize,
    events: usize,
    maxevents: usize,
    timeout: usize,
) -> Result<usize, isize> {
    syscall(232, &[epfd, events, maxevents, timeout])
}

#[inline(always)]
pub unsafe fn epoll_ctl(
    epfd: usize,
    op: usize,
    fd: usize,
    event: usize,
) -> Result<usize, isize> {
    syscall(233, &[epfd, op, fd, event])
}

#[inline(always)]
pub unsafe fn tgkill(tgid: usize, pid: usize, sig: usize) -> Result<usize, isize> {
    syscall(234, &[tgid, pid, sig])
}

#[inline(always)]
pub unsafe fn utimes(filename: usize, utimes: usize) -> Result<usize, isize> {
    syscall(235, &[filename, utimes])
}

#[inline(always)]
pub unsafe fn mbind(
    start: usize,
    len: usize,
    mode: usize,
    nmask: usize,
    maxnode: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(237, &[start, len, mode, nmask, maxnode, flags])
}

#[inline(always)]
pub unsafe fn set_mempolicy(
    mode: usize,
    nmask: usize,
    maxnode: usize,
) -> Result<usize, isize> {
    syscall(238, &[mode, nmask, maxnode])
}

#[inline(always)]
pub unsafe fn get_mempolicy(
    policy: usize,
    nmask: usize,
    maxnode: usize,
    addr: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(239, &[policy, nmask, maxnode, addr, flags])
}

#[inline(always)]
pub unsafe fn mq_open(
    u_name: usize,
    oflag: usize,
    mode: usize,
    u_attr: usize,
) -> Result<usize, isize> {
    syscall(240, &[u_name, oflag, mode, u_attr])
}

#[inline(always)]
pub unsafe fn mq_unlink(u_name: usize) -> Result<usize, isize> {
    syscall(241, &[u_name])
}

#[inline(always)]
pub unsafe fn mq_timesend(
    mqdes: usize,
    u_msg_ptr: usize,
    msg_len: usize,
    msg_prio: usize,
    u_abs_timeout: usize,
) -> Result<usize, isize> {
    syscall(242, &[mqdes, u_msg_ptr, msg_len, msg_prio, u_abs_timeout])
}

#[inline(always)]
pub unsafe fn mq_timedreceive(
    mqdes: usize,
    u_msg_ptr: usize,
    msg_len: usize,
    u_msg_prio: usize,
    u_abs_timeout: usize,
) -> Result<usize, isize> {
    syscall(243, &[mqdes, u_msg_ptr, msg_len, u_msg_prio, u_abs_timeout])
}

#[inline(always)]
pub unsafe fn mq_notify(mqdes: usize, u_notification: usize) -> Result<usize, isize> {
    syscall(244, &[mqdes, u_notification])
}

#[inline(always)]
pub unsafe fn mq_getsetattr(
    mqdes: usize,
    u_mqstat: usize,
    u_omqstat: usize,
) -> Result<usize, isize> {
    syscall(245, &[mqdes, u_mqstat, u_omqstat])
}

#[inline(always)]
pub unsafe fn kexec_load(
    entry: usize,
    nr_segments: usize,
    segments: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(246, &[entry, nr_segments, segments, flags])
}

#[inline(always)]
pub unsafe fn waitid(
    which: usize,
    upid: usize,
    infop: usize,
    options: usize,
    ru: usize,
) -> Result<usize, isize> {
    syscall(247, &[which, upid, infop, options, ru])
}

#[allow(clippy::used_underscore_binding)]
#[inline(always)]
pub unsafe fn add_key(
    _type: usize,
    _description: usize,
    _payload: usize,
    plen: usize,
    ringid: usize,
) -> Result<usize, isize> {
    syscall(248, &[_type, _description, _payload, plen, ringid])
}

#[allow(clippy::used_underscore_binding)]
#[inline(always)]
pub unsafe fn request_key(
    _type: usize,
    _description: usize,
    _callout_info: usize,
    destringid: usize,
) -> Result<usize, isize> {
    syscall(249, &[_type, _description, _callout_info, destringid])
}

#[inline(always)]
pub unsafe fn keyctl(
    option: usize,
    arg2: usize,
    arg3: usize,
    arg4: usize,
    arg5: usize,
) -> Result<usize, isize> {
    syscall(250, &[option, arg2, arg3, arg4, arg5])
}

#[inline(always)]
pub unsafe fn ioprio_set(which: usize, who: usize, ioprio: usize) -> Result<usize, isize> {
    syscall(251, &[which, who, ioprio])
}

#[inline(always)]
pub unsafe fn ioprio_get(which: usize, who: usize) -> Result<usize, isize> {
    syscall(252, &[which, who])
}

#[inline(always)]
pub unsafe fn inotify_init() -> Result<usize, isize> {
    syscall(253, &[])
}

#[inline(always)]
pub unsafe fn inotify_add_watch(
    fd: usize,
    pathname: usize,
    mask: usize,
) -> Result<usize, isize> {
    syscall(254, &[fd, pathname, mask])
}

#[inline(always)]
pub unsafe fn inotify_rm_watch(fd: usize, wd: usize) -> Result<usize, isize> {
    syscall(255, &[fd, wd])
}

#[inline(always)]
pub unsafe fn migrate_pages(
    pid: usize,
    maxnode: usize,
    old_nodes: usize,
    new_nodes: usize,
) -> Result<usize, isize> {
    syscall(256, &[pid, maxnode, old_nodes, new_nodes])
}

#[inline(always)]
pub unsafe fn openat(
    dfd: usize,
    filename: usize,
    flags: usize,
    mode: usize,
) -> Result<usize, isize> {
    syscall(257, &[dfd, filename, flags, mode])
}

#[inline(always)]
pub unsafe fn mkdirat(dfd: usize, pathname: usize, mode: usize) -> Result<usize, isize> {
    syscall(258, &[dfd, pathname, mode])
}

#[inline(always)]
pub unsafe fn mknodat(
    dfd: usize,
    filename: usize,
    mode: usize,
    dev: usize,
) -> Result<usize, isize> {
    syscall(259, &[dfd, filename, mode, dev])
}

#[inline(always)]
pub unsafe fn fchownat(
    dfd: usize,
    filename: usize,
    user: usize,
    group: usize,
    flag: usize,
) -> Result<usize, isize> {
    syscall(260, &[dfd, filename, user, group, flag])
}

#[inline(always)]
pub unsafe fn futimesat(dfd: usize, filename: usize, utimes: usize) -> Result<usize, isize> {
    syscall(261, &[dfd, filename, utimes])
}

#[inline(always)]
pub unsafe fn newfstatat(
    dfd: usize,
    filename: usize,
    statbuf: usize,
    flag: usize,
) -> Result<usize, isize> {
    syscall(262, &[dfd, filename, statbuf, flag])
}

#[inline(always)]
pub unsafe fn unlinkat(dfd: usize, pathname: usize, flag: usize) -> Result<usize, isize> {
    syscall(263, &[dfd, pathname, flag])
}

#[inline(always)]
pub unsafe fn renameat(
    olddfd: usize,
    oldname: usize,
    newdfd: usize,
    newname: usize,
) -> Result<usize, isize> {
    syscall(264, &[olddfd, oldname, newdfd, newname])
}

#[inline(always)]
pub unsafe fn linkat(
    olddfd: usize,
    oldname: usize,
    newdfd: usize,
    newname: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(265, &[olddfd, oldname, newdfd, newname, flags])
}

#[inline(always)]
pub unsafe fn symlinkat(
    oldname: usize,
    newdfd: usize,
    newname: usize,
) -> Result<usize, isize> {
    syscall(266, &[oldname, newdfd, newname])
}

#[inline(always)]
pub unsafe fn readlinkat(
    dfd: usize,
    pathname: usize,
    buf: usize,
    bufsiz: usize,
) -> Result<usize, isize> {
    syscall(267, &[dfd, pathname, buf, bufsiz])
}

#[inline(always)]
pub unsafe fn fchmodat(dfd: usize, filename: usize, mode: usize) -> Result<usize, isize> {
    syscall(268, &[dfd, filename, mode])
}

#[inline(always)]
pub unsafe fn faccessat(dfd: usize, filename: usize, mode: usize) -> Result<usize, isize> {
    syscall(269, &[dfd, filename, mode])
}

#[inline(always)]
pub unsafe fn pselect6(
    n: usize,
    inp: usize,
    outp: usize,
    exp: usize,
    tsp: usize,
    sig: usize,
) -> Result<usize, isize> {
    syscall(270, &[n, inp, outp, exp, tsp, sig])
}

#[inline(always)]
pub unsafe fn ppoll(
    ufds: usize,
    nfds: usize,
    tsp: usize,
    sigmask: usize,
    sigsetsize: usize,
) -> Result<usize, isize> {
    syscall(271, &[ufds, nfds, tsp, sigmask, sigsetsize])
}

#[inline(always)]
pub unsafe fn unshare(unshare_flags: usize) -> Result<usize, isize> {
    syscall(272, &[unshare_flags])
}

#[inline(always)]
pub unsafe fn set_robust_list(head: usize, len: usize) -> Result<usize, isize> {
    syscall(273, &[head, len])
}

#[inline(always)]
pub unsafe fn get_robust_list(
    pid: usize,
    head_ptr: usize,
    len_ptr: usize,
) -> Result<usize, isize> {
    syscall(274, &[pid, head_ptr, len_ptr])
}

#[inline(always)]
pub unsafe fn splice(
    fd_in: usize,
    off_in: usize,
    fd_out: usize,
    off_out: usize,
    len: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(275, &[fd_in, off_in, fd_out, off_out, len, flags])
}

#[inline(always)]
pub unsafe fn tee(
    fdin: usize,
    fdout: usize,
    len: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(276, &[fdin, fdout, len, flags])
}

#[inline(always)]
pub unsafe fn sync_file_range(
    fd: usize,
    offset: usize,
    nbytes: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(277, &[fd, offset, nbytes, flags])
}

#[inline(always)]
pub unsafe fn vmsplice(
    fd: usize,
    iov: usize,
    nr_segs: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(278, &[fd, iov, nr_segs, flags])
}

#[inline(always)]
pub unsafe fn move_pages(
    pid: usize,
    nr_pages: usize,
    pages: usize,
    nodes: usize,
    status: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(279, &[pid, nr_pages, pages, nodes, status, flags])
}

#[inline(always)]
pub unsafe fn utimensat(
    dfd: usize,
    filename: usize,
    utimes: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(280, &[dfd, filename, utimes, flags])
}

#[inline(always)]
pub unsafe fn epoll_pwait(
    epfd: usize,
    events: usize,
    maxevents: usize,
    timeout: usize,
    sigmask: usize,
    sigsetsize: usize,
) -> Result<usize, isize> {
    syscall(
        281,
        &[epfd, events, maxevents, timeout, sigmask, sigsetsize],
    )
}

#[inline(always)]
pub unsafe fn signalfd(ufd: usize, user_mask: usize, sizemask: usize) -> Result<usize, isize> {
    syscall(282, &[ufd, user_mask, sizemask])
}

#[inline(always)]
pub unsafe fn timerfd_create(clockid: usize, flags: usize) -> Result<usize, isize> {
    syscall(283, &[clockid, flags])
}

#[inline(always)]
pub unsafe fn eventfd(count: usize) -> Result<usize, isize> {
    syscall(284, &[count])
}

#[inline(always)]
pub unsafe fn fallocate(
    fd: usize,
    mode: usize,
    offset: usize,
    len: usize,
) -> Result<usize, isize> {
    syscall(285, &[fd, mode, offset, len])
}

#[inline(always)]
pub unsafe fn timerfd_settime(
    ufd: usize,
    flags: usize,
    utmr: usize,
    otmr: usize,
) -> Result<usize, isize> {
    syscall(286, &[ufd, flags, utmr, otmr])
}

#[inline(always)]
pub unsafe fn timerfd_gettime(ufd: usize, otmr: usize) -> Result<usize, isize> {
    syscall(287, &[ufd, otmr])
}

#[inline(always)]
pub unsafe fn accept4(
    fd: usize,
    upeer_sockaddr: usize,
    upeer_addrlen: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(288, &[fd, upeer_sockaddr, upeer_addrlen, flags])
}

#[inline(always)]
pub unsafe fn signalfd4(
    ufd: usize,
    user_mask: usize,
    sizemask: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(289, &[ufd, user_mask, sizemask, flags])
}

#[inline(always)]
pub unsafe fn eventfd2(count: usize, flags: usize) -> Result<usize, isize> {
    syscall(290, &[count, flags])
}

#[inline(always)]
pub unsafe fn epoll_create1(flags: usize) -> Result<usize, isize> {
    syscall(291, &[flags])
}

#[inline(always)]
pub unsafe fn dup3(oldfd: usize, newfd: usize, flags: usize) -> Result<usize, isize> {
    syscall(292, &[oldfd, newfd, flags])
}

#[inline(always)]
pub unsafe fn pipe2(fildes: usize, flags: usize) -> Result<usize, isize> {
    syscall(293, &[fildes, flags])
}

#[inline(always)]
pub unsafe fn inotify_init1(flags: usize) -> Result<usize, isize> {
    syscall(294, &[flags])
}

#[inline(always)]
pub unsafe fn preadv(
    fd: usize,
    vec: usize,
    vlen: usize,
    pos_l: usize,
    pos_h: usize,
) -> Result<usize, isize> {
    syscall(295, &[fd, vec, vlen, pos_l, pos_h])
}

#[inline(always)]
pub unsafe fn pwritev(
    fd: usize,
    vec: usize,
    vlen: usize,
    pos_l: usize,
    pos_h: usize,
) -> Result<usize, isize> {
    syscall(296, &[fd, vec, vlen, pos_l, pos_h])
}

#[inline(always)]
pub unsafe fn rt_tgsigqueueinfo(
    tgid: usize,
    pid: usize,
    sig: usize,
    uinfo: usize,
) -> Result<usize, isize> {
    syscall(297, &[tgid, pid, sig, uinfo])
}

#[inline(always)]
pub unsafe fn perf_event_open(
    attr_uptr: usize,
    pid: usize,
    cpu: usize,
    group_fd: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(298, &[attr_uptr, pid, cpu, group_fd, flags])
}

#[inline(always)]
pub unsafe fn recvmmsg(
    fd: usize,
    mmsg: usize,
    vlen: usize,
    flags: usize,
    timeout: usize,
) -> Result<usize, isize> {
    syscall(299, &[fd, mmsg, vlen, flags, timeout])
}

#[inline(always)]
pub unsafe fn fanotify_init(flags: usize, event_f_flags: usize) -> Result<usize, isize> {
    syscall(300, &[flags, event_f_flags])
}

#[inline(always)]
pub unsafe fn fanotify_mark(
    fanotify_fd: usize,
    flags: usize,
    mask: usize,
    dfd: usize,
    pathname: usize,
) -> Result<usize, isize> {
    syscall(301, &[fanotify_fd, flags, mask, dfd, pathname])
}

#[inline(always)]
pub unsafe fn prlimit64(
    pid: usize,
    resource: usize,
    new_rlim: usize,
    old_rlim: usize,
) -> Result<usize, isize> {
    syscall(302, &[pid, resource, new_rlim, old_rlim])
}

#[inline(always)]
pub unsafe fn name_to_handle_at(
    dfd: usize,
    name: usize,
    handle: usize,
    mnt_id: usize,
    flag: usize,
) -> Result<usize, isize> {
    syscall(303, &[dfd, name, handle, mnt_id, flag])
}

#[inline(always)]
pub unsafe fn open_by_handle_at(
    mountdirfd: usize,
    handle: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(304, &[mountdirfd, handle, flags])
}

#[inline(always)]
pub unsafe fn clock_adjtime(which_clock: usize, utx: usize) -> Result<usize, isize> {
    syscall(305, &[which_clock, utx])
}

#[inline(always)]
pub unsafe fn syncfs(fd: usize) -> Result<usize, isize> {
    syscall(306, &[fd])
}

#[inline(always)]
pub unsafe fn sendmmsg(
    fd: usize,
    mmsg: usize,
    vlen: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(307, &[fd, mmsg, vlen, flags])
}

#[inline(always)]
pub unsafe fn setns(fd: usize, nstype: usize) -> Result<usize, isize> {
    syscall(308, &[fd, nstype])
}

#[inline(always)]
pub unsafe fn getcpu(cpup: usize, nodep: usize, unused: usize) -> Result<usize, isize> {
    syscall(309, &[cpup, nodep, unused])
}

#[inline(always)]
pub unsafe fn process_vm_readv(
    pid: usize,
    lvec: usize,
    liovcnt: usize,
    rvec: usize,
    riovcnt: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(310, &[pid, lvec, liovcnt, rvec, riovcnt, flags])
}

#[inline(always)]
pub unsafe fn process_vm_writev(
    pid: usize,
    lvec: usize,
    liovcnt: usize,
    rvec: usize,
    riovcnt: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(311, &[pid, lvec, liovcnt, rvec, riovcnt, flags])
}

#[inline(always)]
pub unsafe fn kcmp(
    pid1: usize,
    pid2: usize,
    r#type: usize,
    idx1: usize,
    idx2: usize,
) -> Result<usize, isize> {
    syscall(312, &[pid1, pid2, r#type, idx1, idx2])
}

#[inline(always)]
pub unsafe fn finit_module(fd: usize, uargs: usize, flags: usize) -> Result<usize, isize> {
    syscall(313, &[fd, uargs, flags])
}

#[inline(always)]
pub unsafe fn sched_setattr(pid: usize, uattr: usize, flags: usize) -> Result<usize, isize> {
    syscall(314, &[pid, uattr, flags])
}

#[inline(always)]
pub unsafe fn sched_getattr(
    pid: usize,
    uattr: usize,
    size: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(315, &[pid, uattr, size, flags])
}

#[inline(always)]
pub unsafe fn renameat2(
    olddfd: usize,
    oldname: usize,
    newdfd: usize,
    newname: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(316, &[olddfd, oldname, newdfd, newname, flags])
}

#[inline(always)]
pub unsafe fn seccomp(op: usize, flags: usize, uargs: usize) -> Result<usize, isize> {
    syscall(317, &[op, flags, uargs])
}

#[inline(always)]
pub unsafe fn getrandom(buf: usize, count: usize, flags: usize) -> Result<usize, isize> {
    syscall(318, &[buf, count, flags])
}

#[inline(always)]
pub unsafe fn memfd_create(uname: usize, flags: usize) -> Result<usize, isize> {
    syscall(319, &[uname, flags])
}

#[inline(always)]
pub unsafe fn kexec_file_load(
    kernel_fd: usize,
    initrd_fd: usize,
    cmdline_len: usize,
    cmdline_ptr: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(
        320,
        &[kernel_fd, initrd_fd, cmdline_len, cmdline_ptr, flags],
    )
}

#[inline(always)]
pub unsafe fn bpf(cmd: usize, uattr: usize, size: usize) -> Result<usize, isize> {
    syscall(321, &[cmd, uattr, size])
}

#[inline(always)]
pub unsafe fn execveat(
    fd: usize,
    filename: usize,
    argv: usize,
    envp: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(322, &[fd, filename, argv, envp, flags])
}

#[inline(always)]
pub unsafe fn userfaultfd(flags: usize) -> Result<usize, isize> {
    syscall(323, &[flags])
}

#[inline(always)]
pub unsafe fn membarrier(cmd: usize, flags: usize) -> Result<usize, isize> {
    syscall(324, &[cmd, flags])
}

#[inline(always)]
pub unsafe fn mlock2(start: usize, len: usize, flags: usize) -> Result<usize, isize> {
    syscall(325, &[start, len, flags])
}

#[inline(always)]
pub unsafe fn copy_file_range(
    fd_in: usize,
    off_in: usize,
    fd_out: usize,
    off_out: usize,
    len: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(326, &[fd_in, off_in, fd_out, off_out, len, flags])
}

#[inline(always)]
pub unsafe fn preadv2(
    fd: usize,
    vec: usize,
    vlen: usize,
    pos_l: usize,
    pos_h: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(327, &[fd, vec, vlen, pos_l, pos_h, flags])
}

#[inline(always)]
pub unsafe fn pwritev2(
    fd: usize,
    vec: usize,
    vlen: usize,
    pos_l: usize,
    pos_h: usize,
    flags: usize,
) -> Result<usize, isize> {
    syscall(328, &[fd, vec, vlen, pos_l, pos_h, flags])
}

#[inline(always)]
pub unsafe fn pkey_mprotect(
    start: usize,
    len: usize,
    prot: usize,
    pkey: usize,
) -> Result<usize, isize> {
    syscall(329, &[start, len, prot, pkey])
}

#[inline(always)]
pub unsafe fn pkey_alloc(flags: usize, val: usize) -> Result<usize, isize> {
    syscall(330, &[flags, val])
}

#[inline(always)]
pub unsafe fn pkey_free(pkey: usize) -> Result<usize, isize> {
    syscall(331, &[pkey])
}

#[inline(always)]
pub unsafe fn statx(
    dfd: usize,
    filename: usize,
    flags: usize,
    mask: usize,
    buffer: usize,
) -> Result<usize, isize> {
    syscall(332, &[dfd, filename, flags, mask, buffer])
}

#[inline(always)]
pub unsafe fn io_pgetevents(
    ctx_id: usize,
    min_nr: usize,
    nr: usize,
    events: usize,
    timeout: usize,
    usig: usize,
) -> Result<usize, isize> {
    syscall(333, &[ctx_id, min_nr, nr, events, timeout, usig])
}

#[inline(always)]
pub unsafe fn rseq(
    rseq: usize,
    rseq_len: usize,
    flags: usize,
    sig: usize,
) -> Result<usize, isize> {
    syscall(334, &[rseq, rseq_len, flags, sig])
}
