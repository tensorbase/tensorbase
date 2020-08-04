use base::errors::*;

/// Command parameter
#[derive(Debug, PartialEq)]
pub struct Parameter {
    pub(crate) name: String,
    pub(crate) required: bool,
    pub(crate) default: Option<String>,
}

impl Parameter {
    /// Create a new command parameter with the given name
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            required: false,
            default: None,
        }
    }

    /// Set whether the parameter is required, default is not required.
    /// Note that you cannot have a required parameter after a non-required one
    pub fn set_required(mut self, required: bool) -> Result<Self> {
        if self.default.is_some() {
            return Err(Error::IllegalRequiredError(self.name));
        }
        self.required = required;

        Ok(self)
    }

    /// Set a default for an optional parameter.
    /// Note that you can't have a default for a required parameter
    pub fn set_default(mut self, default: &str) -> Result<Self> {
        if self.required {
            return Err(Error::IllegalDefaultError(self.name));
        }
        self.default = Some(default.to_string());

        Ok(self)
    }
}
