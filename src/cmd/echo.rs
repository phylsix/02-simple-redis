use crate::{Backend, RespArray, RespFrame};

use super::{extract_args, validate_command, CommandError, CommandExecutor, Echo};

impl CommandExecutor for Echo {
    fn execute(self, _: &Backend) -> RespFrame {
        RespFrame::BulkString(self.message.into())
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["echo"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(message)) => Ok(Echo {
                message: String::from_utf8(message.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid message".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespDecode;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_echo() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n");

        let frame = RespArray::decode(&mut buf)?;
        let cmd = Echo::try_from(frame)?;

        assert_eq!(cmd.message, "hello world");
        Ok(())
    }
}
