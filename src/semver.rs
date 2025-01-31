use regex::Regex;

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct Semver {
    pub mayor: u64,
    pub minor: u64,
    pub patch: u64,
    pub extra: String,
}

impl Semver {
    pub fn new(mayor: u64, minor: u64, patch: u64) -> Semver {
        Semver {
            mayor,
            minor,
            patch,
            extra: "".to_string(),
        }
    }

    pub fn parse(value: &str) -> Result<Semver, anyhow::Error> {
        Regex::new(r"^(?P<mayor>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?P<extra>.*)?$")?
            .captures(value)
            .map(|captures| Semver {
                mayor: captures.name("mayor").unwrap().as_str().parse().unwrap(),
                minor: captures.name("minor").unwrap().as_str().parse().unwrap(),
                patch: captures.name("patch").unwrap().as_str().parse().unwrap(),
                extra: captures
                    .name("extra")
                    .map_or("", |m| m.as_str())
                    .to_string(),
            })
            .ok_or_else(|| anyhow::anyhow!("Invalid semver"))
    }
}

impl std::fmt::Display for Semver {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.mayor, self.minor, self.patch)?;
        if !self.extra.is_empty() {
            write!(f, "{}", self.extra)?;
        }
        Ok(())
    }
}
