extern crate chrono;
extern crate failure;

#[macro_use]
extern crate failure_derive;

use chrono::DateTime;
use chrono::Utc;
use failure::Error;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;

#[derive(Debug, Fail)]
pub enum CondvarStoreError {
    #[fail(display = "Timeout while waiting for GET")]
    GetTimeout,
    #[fail(display = "Poisoned lock: {}", _0)]
    PoisonedLock(String),
}

pub trait GetExpiry {
    fn get(&mut self) -> Result<(), Error>;
    fn expiry(&self) -> DateTime<Utc>;
}

#[derive(Debug)]
pub struct CondvarStore<T: GetExpiry> {
    pub cached: Arc<RwLock<T>>,
    pub is_inflight: Arc<(Mutex<bool>, Condvar)>,
    pub expiry: Arc<RwLock<DateTime<Utc>>>,
    pub timeout: u64,
}

impl<T: GetExpiry> Clone for CondvarStore<T> {
    fn clone(&self) -> Self {
        CondvarStore {
            cached: Arc::clone(&self.cached),
            is_inflight: Arc::clone(&self.is_inflight),
            expiry: Arc::clone(&self.expiry),
            timeout: self.timeout,
        }
    }
}

impl<T: GetExpiry> CondvarStore<T> {
    #[allow(clippy::mutex_atomic)]
    pub fn new(t: T) -> Self {
        let expiry = t.expiry();
        CondvarStore {
            cached: Arc::new(RwLock::new(t)),
            is_inflight: Arc::new((Mutex::new(false), Condvar::new())),
            expiry: Arc::new(RwLock::new(expiry)),
            timeout: 1000,
        }
    }

    pub fn with_timeout(mut self, t: u64) -> Self {
        self.timeout = t;
        self
    }

    fn update(&self) -> Result<(), Error> {
        let mut k = self
            .cached
            .write()
            .map_err(|e| CondvarStoreError::PoisonedLock(e.to_string()))?;
        k.get()?;
        let mut expiry = self
            .expiry
            .write()
            .map_err(|e| CondvarStoreError::PoisonedLock(e.to_string()))?;
        *expiry = k.expiry();
        Ok(())
    }

    pub fn get(&self) -> Result<Arc<RwLock<T>>, Error> {
        let now = Utc::now();
        if let Ok(expiry) = self.expiry.read() {
            if *expiry > now {
                return Ok(Arc::clone(&self.cached));
            }
        }
        let inflight_pair = self.is_inflight.clone();
        let &(ref lock, ref cvar) = &*inflight_pair;
        let chosen = {
            let mut is_inflight = lock
                .lock()
                .map_err(|e| CondvarStoreError::PoisonedLock(e.to_string()))?;
            if !*is_inflight {
                *is_inflight = true;
                true
            } else {
                false
            }
        };
        if chosen {
            let updated = self.update();
            let mut is_inflight = lock
                .lock()
                .map_err(|e| CondvarStoreError::PoisonedLock(e.to_string()))?;
            assert!(*is_inflight);
            *is_inflight = false;
            updated?;
        }
        let mut is_inflight = lock
            .lock()
            .map_err(|e| CondvarStoreError::PoisonedLock(e.to_string()))?;
        while *is_inflight {
            let r = cvar
                .wait_timeout(is_inflight, Duration::from_millis(self.timeout))
                .map_err(|e| CondvarStoreError::PoisonedLock(e.to_string()))?;
            if r.1.timed_out() {
                return Err(CondvarStoreError::GetTimeout.into());
            }
            is_inflight = r.0;
        }
        Ok(Arc::clone(&self.cached))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;
    use std::thread;

    #[derive(Debug, Clone)]
    struct R1 {
        pub r: usize,
        pub exp: DateTime<Utc>,
    }

    impl GetExpiry for R1 {
        fn get(&mut self) -> Result<(), Error> {
            self.r += 1;
            self.exp = Utc::now();
            Ok(())
        }
        fn expiry(&self) -> DateTime<Utc> {
            self.exp
        }
    }

    #[derive(Debug, Clone)]
    struct R2 {
        pub r: usize,
        pub exp: DateTime<Utc>,
    }

    impl GetExpiry for R2 {
        fn get(&mut self) -> Result<(), Error> {
            self.r += 1;
            self.exp = Utc::now() + chrono::Duration::seconds(10);
            Ok(())
        }
        fn expiry(&self) -> DateTime<Utc> {
            self.exp
        }
    }

    #[derive(Debug, Clone)]
    struct RTimeout {
        pub r: usize,
        pub exp: DateTime<Utc>,
    }

    impl GetExpiry for RTimeout {
        fn get(&mut self) -> Result<(), Error> {
            thread::sleep(Duration::from_millis(10));
            self.r += 1;
            self.exp = Utc::now() + chrono::Duration::seconds(10);
            Ok(())
        }
        fn expiry(&self) -> DateTime<Utc> {
            self.exp
        }
    }

    #[derive(Debug, Clone)]
    struct RError {
        pub r: usize,
        pub e: usize,
        pub exp: DateTime<Utc>,
    }

    impl GetExpiry for RError {
        fn get(&mut self) -> Result<(), Error> {
            thread::sleep(Duration::from_millis(10));
            self.e += 1;
            if self.e < 2 {
                return Err(CondvarStoreError::GetTimeout.into());
            }
            self.r += 1;
            self.exp = Utc::now() + chrono::Duration::seconds(10);
            Ok(())
        }
        fn expiry(&self) -> DateTime<Utc> {
            self.exp
        }
    }

    fn check_ok_and_expiry<T: GetExpiry + Clone>(ks: &CondvarStore<T>, now: &DateTime<Utc>) -> T {
        let k = ks.get();
        assert!(k.is_ok());
        let k = k.unwrap();
        let k = k.read().unwrap();
        assert!(k.expiry() > *now);
        k.clone()
    }

    #[test]
    fn test_get_from() -> Result<(), Error> {
        let r1 = R1 {
            r: 0,
            exp: Utc.timestamp(0, 0),
        };
        let now = Utc::now();
        let ks = Arc::new(CondvarStore::<R1>::new(r1));
        let ks_c = Arc::clone(&ks);
        let child = thread::spawn(move || {
            check_ok_and_expiry(&ks_c, &now);
        });
        check_ok_and_expiry(&ks, &now);
        let _ = child.join();
        let k = check_ok_and_expiry(&ks, &now);
        assert_eq!(k.r, 3);
        Ok(())
    }

    #[test]
    fn test_get_from_cached_timeout() {
        let r_timeout = RTimeout {
            r: 0,
            exp: Utc.timestamp(0, 0),
        };
        let now = Utc::now();
        let ks = Arc::new(CondvarStore::<RTimeout>::new(r_timeout).with_timeout(2));
        let ks_c = Arc::clone(&ks);
        let child = thread::spawn(move || {
            check_ok_and_expiry(&ks_c, &now);
        });
        thread::sleep(Duration::from_millis(5));
        {
            let k = ks.get();
            assert!(k.is_err());
        }
        let _ = child.join();
        let k = check_ok_and_expiry(&ks, &now);
        assert_eq!(k.r, 1);
    }

    #[test]
    fn test_get_from_cached_error_getting() {
        let r_error = RError {
            r: 0,
            e: 0,
            exp: Utc.timestamp(0, 0),
        };
        let now = Utc::now();
        let ks = Arc::new(CondvarStore::<RError>::new(r_error).with_timeout(2));
        let ks_c = Arc::clone(&ks);
        let child = thread::spawn(move || {
            let k = ks_c.get();
            assert!(k.is_err());
        });
        {
            let k = ks.get();
            assert!(k.is_err());
        }
        let _ = child.join();
        let k = ks.get();
        assert!(k.is_ok());
        let k = k.unwrap();
        let k = k.read().unwrap();
        assert!(k.expiry() > now);
        assert_eq!(k.r, 1);
    }

    #[test]
    fn test_get_from_cached() {
        let r2 = R2 {
            r: 0,
            exp: Utc.timestamp(0, 0),
        };
        let now = Utc::now();
        assert!(Utc::now() + chrono::Duration::seconds(10) > now);
        let ks = Arc::new(CondvarStore::<R2>::new(r2));
        let ks_c = Arc::clone(&ks);
        let child = thread::spawn(move || {
            check_ok_and_expiry(&ks_c, &now);
        });
        check_ok_and_expiry(&ks, &now);
        let _ = child.join();
        let k = check_ok_and_expiry(&ks, &now);
        assert_eq!(k.r, 1);
    }
}
