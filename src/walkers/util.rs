use crate::errors::FSError;

#[derive(Debug)]
pub(super) enum Output<J, T> {
    ToPush(Vec<J>),
    ToSend(Result<T, FSError>),
    Nil,
}
