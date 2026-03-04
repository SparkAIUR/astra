#[derive(Debug, Clone, Copy)]
pub enum IoMode {
    IoUring,
    Posix,
}

#[derive(Debug, Clone)]
pub struct HalProfile {
    pub arch: String,
    pub kernel_release: Option<String>,
    pub io_mode: IoMode,
    pub sse42: bool,
    pub avx2: bool,
    pub neon: bool,
}

impl HalProfile {
    pub fn detect() -> Self {
        let arch = std::env::consts::ARCH.to_string();
        let kernel_release = detect_kernel_release();

        #[cfg(target_arch = "x86_64")]
        let sse42 = std::is_x86_feature_detected!("sse4.2");
        #[cfg(not(target_arch = "x86_64"))]
        let sse42 = false;

        #[cfg(target_arch = "x86_64")]
        let avx2 = std::is_x86_feature_detected!("avx2");
        #[cfg(not(target_arch = "x86_64"))]
        let avx2 = false;

        #[cfg(target_arch = "aarch64")]
        let neon = std::arch::is_aarch64_feature_detected!("neon");
        #[cfg(not(target_arch = "aarch64"))]
        let neon = false;

        let io_mode = if supports_io_uring(kernel_release.as_deref()) {
            IoMode::IoUring
        } else {
            IoMode::Posix
        };

        Self {
            arch,
            kernel_release,
            io_mode,
            sse42,
            avx2,
            neon,
        }
    }

    pub fn startup_line(&self) -> String {
        let io = match self.io_mode {
            IoMode::IoUring => "io_uring",
            IoMode::Posix => "epoll/preadv/pwritev fallback",
        };

        format!(
            "HAL detected arch={} kernel={:?} io_mode={} sse4.2={} avx2={} neon={}",
            self.arch, self.kernel_release, io, self.sse42, self.avx2, self.neon
        )
    }
}

fn detect_kernel_release() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .map(|s| s.trim().to_string())
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn supports_io_uring(kernel_release: Option<&str>) -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = kernel_release;
        return false;
    }

    #[cfg(target_os = "linux")]
    {
        let Some(release) = kernel_release else {
            return false;
        };

        let mut parts = release
            .split(['.', '-'])
            .filter_map(|p| p.parse::<u64>().ok());

        let major = parts.next().unwrap_or(0);
        let minor = parts.next().unwrap_or(0);

        major > 5 || (major == 5 && minor >= 1)
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[multiversion::multiversion(targets("x86_64+avx2", "x86_64+sse4.2", "aarch64+neon"))]
pub fn simd_accumulate(bytes: &[u8]) -> u64 {
    bytes.iter().map(|b| u64::from(*b)).sum()
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn simd_accumulate(bytes: &[u8]) -> u64 {
    bytes.iter().map(|b| u64::from(*b)).sum()
}
