use sealed::Permission;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::convert::Infallible;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use zeroize::__internal::AssertZeroize;
use zeroize::{Zeroize, ZeroizeOnDrop};

mod sealed {
    pub trait Permission {}
    pub trait SecurityLevel {}
}

struct Allow;
struct Deny;
impl Permission for Allow {}
impl Permission for Deny {}

trait MaybeZeroize<T> {
    fn maybe_zeroize(inner: &mut T);
}

struct Zeroizer<T: Zeroize>(PhantomData<T>);
impl<T: Zeroize> MaybeZeroize<T> for Zeroizer<T> {
    fn maybe_zeroize(inner: &mut T) {
        inner.zeroize_or_on_drop()
    }
}

impl<T> MaybeZeroize<T> for () {
    fn maybe_zeroize(_: &mut T) {
        // do nothing
    }
}

trait Security: sealed::SecurityLevel {
    type ExtractPermission: Permission;
    type SerializationPermission: Permission;
    type ClonePermission: Permission;
    type ComparePermission: Permission;
    type SupportedType;
    type MaybeZeroize: MaybeZeroize<Self::SupportedType>;
}

pub struct ConfidentialSecurity<T>(PhantomData<T>);
impl<T: Zeroize + Sized> sealed::SecurityLevel for ConfidentialSecurity<T> {}
impl<T: Zeroize + Sized> Security for ConfidentialSecurity<T> {
    type ExtractPermission = Deny;
    type SerializationPermission = Deny;
    type ClonePermission = Deny;
    type ComparePermission = Deny;
    type SupportedType = T;
    type MaybeZeroize = Zeroizer<T>;
}

pub struct ProtectedSecurity<T>(PhantomData<T>);
impl<T: Zeroize + Sized> sealed::SecurityLevel for ProtectedSecurity<T> {}
impl<T: Zeroize + Sized> Security for ProtectedSecurity<T> {
    type ExtractPermission = Deny;
    type SerializationPermission = Allow;
    type ClonePermission = Allow;
    type ComparePermission = Allow;
    type SupportedType = T;
    type MaybeZeroize = Zeroizer<T>;
}

pub struct SensitiveSecurity<T>(PhantomData<T>);
impl<T: Sized> sealed::SecurityLevel for SensitiveSecurity<T> {}
impl<T: Sized> Security for SensitiveSecurity<T> {
    type ExtractPermission = Allow;
    type SerializationPermission = Allow;
    type ClonePermission = Allow;
    type ComparePermission = Allow;
    type SupportedType = T;
    type MaybeZeroize = ();
}

pub type Confidential<T> = Veil<T, ConfidentialSecurity<T>>;

impl<T: Zeroize + Sized> ZeroizeOnDrop for Confidential<T> {}

pub type Protected<T> = Veil<T, ProtectedSecurity<T>>;

impl<T: Zeroize + Sized> ZeroizeOnDrop for Protected<T> {}

pub type Sensitive<T> = Veil<T, SensitiveSecurity<T>>;

impl<T: ZeroizeOnDrop + Sized> ZeroizeOnDrop for Sensitive<T> {}

#[repr(transparent)]
#[allow(private_bounds)]
pub struct Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    inner: Option<T>,
    _phantom_data: PhantomData<S>,
}

#[allow(private_bounds)]
impl<T, S> Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner: Some(inner),
            _phantom_data: PhantomData,
        }
    }
}

#[allow(private_bounds)]
impl<T, S> Veil<T, S>
where
    S: Security<ExtractPermission = Allow, SupportedType = T>,
{
    pub fn extract_secret(mut self) -> T {
        self.inner.take().unwrap()
    }
}

impl<T: Clone, S> Clone for Veil<T, S>
where
    S: Security<ClonePermission = Allow, SupportedType = T>,
{
    fn clone(&self) -> Self {
        Self {
            inner: Some(self.inner.as_ref().unwrap().clone()),
            _phantom_data: PhantomData,
        }
    }
}

impl<T: PartialEq, S> PartialEq for Veil<T, S>
where
    S: Security<ComparePermission = Allow, SupportedType = T>,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<T: PartialOrd, S> PartialOrd for Veil<T, S>
where
    S: Security<ComparePermission = Allow, SupportedType = T>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T: Debug, S> Debug for Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.serialize_str("[redacted]")
    }
}

impl<T: Display, S> Display for Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.serialize_str("[redacted]")
    }
}

impl<T: Serialize, Sec> Serialize for Veil<T, Sec>
where
    Sec: Security<SerializationPermission = Allow, SupportedType = T>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.as_ref().unwrap().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>, S> Deserialize<'de> for Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(|d| Self::new(d))
    }
}

impl<T: Zeroize, S> Zeroize for Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    #[inline]
    fn zeroize(&mut self) {
        if let Some(inner) = self.inner.as_mut() {
            inner.zeroize();
        }
    }
}

impl<T, S> Drop for Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    fn drop(&mut self) {
        if let Some(inner) = self.inner.as_mut() {
            S::MaybeZeroize::maybe_zeroize(inner)
        }
    }
}

impl<'a, T: 'a, S> SecretKeeper<'a, T> for Veil<T, S>
where
    S: Security<SupportedType = T>,
{
    type Secret = &'a T;
    type SecretMut = &'a mut T;
    type RevealPermit = ();
    type RevealMutPermit = ();
    type Error = Infallible;

    #[inline]
    fn try_reveal(&'a self, _permit: &Self::RevealPermit) -> Result<Self::Secret, Self::Error> {
        Ok(self.inner.as_ref().unwrap())
    }

    #[inline]
    fn try_reveal_mut(
        &'a mut self,
        _permit: &Self::RevealMutPermit,
    ) -> Result<Self::SecretMut, Self::Error> {
        Ok(self.inner.as_mut().unwrap())
    }
}

pub trait SecretKeeper<'a, T: ?Sized> {
    type Secret: Deref<Target = T>;
    type SecretMut: DerefMut<Target = T>;
    type RevealPermit;
    type RevealMutPermit;
    type Error;

    fn try_reveal(&'a self, permit: &Self::RevealPermit) -> Result<Self::Secret, Self::Error>;

    fn try_reveal_mut(
        &'a mut self,
        permit: &Self::RevealMutPermit,
    ) -> Result<Self::SecretMut, Self::Error>;
}

pub trait NewSecretExt {
    fn confidential(self) -> Confidential<Self>
    where
        Self: Zeroize + Sized;

    fn protected(self) -> Protected<Self>
    where
        Self: Zeroize + Sized;

    fn sensitive(self) -> Sensitive<Self>
    where
        Self: Sized;
}

impl<T> NewSecretExt for T {
    fn confidential(self) -> Confidential<T>
    where
        Self: Zeroize + Sized,
    {
        Confidential::new(self)
    }

    fn protected(self) -> Protected<Self>
    where
        Self: Zeroize + Sized,
    {
        Protected::new(self)
    }

    fn sensitive(self) -> Sensitive<Self>
    where
        Self: Sized,
    {
        Sensitive::new(self)
    }
}

pub trait RevealExt<'a, T, S: SecretKeeper<'a, T>> {
    fn reveal(&'a self) -> S::Secret;
}

impl<'a, T, S: SecretKeeper<'a, T, Error = Infallible, RevealPermit = ()>> RevealExt<'a, T, S>
    for S
{
    #[inline]
    fn reveal(&'a self) -> S::Secret {
        self.try_reveal(&()).expect("Infallible")
    }
}

pub trait RevealMutExt<'a, T, S: SecretKeeper<'a, T>> {
    fn reveal_mut(&'a mut self) -> S::SecretMut;
}

impl<'a, T, S: SecretKeeper<'a, T, Error = Infallible, RevealMutPermit = ()>> RevealMutExt<'a, T, S>
    for S
{
    #[inline]
    fn reveal_mut(&'a mut self) -> S::SecretMut {
        self.try_reveal_mut(&()).expect("Infallible")
    }
}

pub trait OptionRevealExt<'a, T, S: SecretKeeper<'a, T>> {
    fn reveal(self) -> Option<S::Secret>;
}

impl<'a, T, S: SecretKeeper<'a, T>> OptionRevealExt<'a, T, S> for Option<&'a S>
where
    S: RevealExt<'a, T, S>,
{
    #[inline]
    fn reveal(self) -> Option<S::Secret> {
        self.map(|s| s.reveal())
    }
}

pub trait OptionRevealMutExt<'a, T, S: SecretKeeper<'a, T>> {
    fn reveal_mut(self) -> Option<S::SecretMut>;
}

impl<'a, T, S: SecretKeeper<'a, T>> OptionRevealMutExt<'a, T, S> for Option<&'a mut S>
where
    S: RevealMutExt<'a, T, S>,
{
    #[inline]
    fn reveal_mut(self) -> Option<S::SecretMut> {
        self.map(|s| s.reveal_mut())
    }
}
