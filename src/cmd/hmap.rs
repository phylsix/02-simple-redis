use super::{
    extract_args, validate_command, CommandExecutor, HGet, HGetAll, HMGet, HSet, SAdd, SIsMember,
    RESP_OK,
};
use crate::{cmd::CommandError, BulkString, RespArray, RespFrame};

impl CommandExecutor for HGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(crate::RespNull),
        }
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let hmap = backend.hmap.get(&self.key);

        match hmap {
            Some(hmap) => {
                let mut data = Vec::with_capacity(hmap.len());
                for v in hmap.iter() {
                    let key = v.key().to_owned();
                    data.push((key, v.value().clone()));
                }
                if self.sort {
                    data.sort_by(|a, b| a.0.cmp(&b.0));
                }
                let ret = data
                    .into_iter()
                    .flat_map(|(k, v)| vec![BulkString::from(k).into(), v])
                    .collect::<Vec<RespFrame>>();

                RespArray::new(ret).into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl CommandExecutor for HMGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let hmap = backend.hmap.get(&self.key);

        match hmap {
            Some(hmap) => {
                let mut data = Vec::with_capacity(self.fields.len());
                for field in &self.fields {
                    if let Some(value) = hmap.get(field) {
                        data.push(value.clone());
                    } else {
                        data.push(RespFrame::Null(crate::RespNull));
                    }
                }

                RespArray::new(data).into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

impl CommandExecutor for SAdd {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.sadd(self.key.as_str(), self.members)
    }
}

impl CommandExecutor for SIsMember {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.sismember(&self.key, &self.member)
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.as_ref().to_vec())?,
                field: String::from_utf8(field.as_ref().to_vec())?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.as_ref().to_vec())?,
                sort: false,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HMGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let args = extract_args(value, 0)?;
        if args.len() < 3 {
            return Err(CommandError::InvalidArgument(
                "command hmget must have at least 2 arguments".to_string(),
            ));
        }
        if args[0] != BulkString::new(b"hmget".to_vec()).into() {
            return Err(CommandError::InvalidArgument(format!(
                "Invalid command: {:?}, expect hmget",
                args[0]
            )));
        }

        let mut args = args.into_iter().skip(1);
        if let Some(RespFrame::BulkString(key)) = args.next() {
            let fields = args
                .map(|f| {
                    if let RespFrame::BulkString(field) = f {
                        Ok(String::from_utf8(field.as_ref().to_vec())?)
                    } else {
                        Err(CommandError::InvalidArgument("Invalid field".to_string()))
                    }
                })
                .collect::<Result<Vec<String>, CommandError>>()?;
            Ok(HMGet {
                key: String::from_utf8(key.as_ref().to_vec())?,
                fields,
            })
        } else {
            Err(CommandError::InvalidArgument("Invalid key".to_string()))
        }
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let args = extract_args(value, 0)?;
        if args.len() < 3 {
            return Err(CommandError::InvalidArgument(
                "command sadd must have at least 2 arguments".to_string(),
            ));
        }
        if args[0] != BulkString::new(b"sadd".to_vec()).into() {
            return Err(CommandError::InvalidArgument(format!(
                "Invalid command: {:?}, expect sadd",
                args[0]
            )));
        }

        let mut args = args.into_iter().skip(1);
        if let Some(RespFrame::BulkString(key)) = args.next() {
            let members = args
                .map(|f| {
                    if let RespFrame::BulkString(field) = f {
                        Ok(String::from_utf8(field.as_ref().to_vec())?)
                    } else {
                        Err(CommandError::InvalidArgument("Invalid member".to_string()))
                    }
                })
                .collect::<Result<Vec<String>, CommandError>>()?;
            Ok(SAdd {
                key: String::from_utf8(key.as_ref().to_vec())?,
                members,
            })
        } else {
            Err(CommandError::InvalidArgument("Invalid key".to_string()))
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.as_ref().to_vec())?,
                    field: String::from_utf8(field.as_ref().to_vec())?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key, field or value".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for SIsMember {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["sismember"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(member))) => {
                Ok(SIsMember {
                    key: String::from_utf8(key.as_ref().to_vec())?,
                    member: String::from_utf8(member.as_ref().to_vec())?,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or member".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecode;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HGet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");

        Ok(())
    }

    #[test]
    fn test_hgetall_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HGetAll = frame.try_into()?;
        assert_eq!(result.key, "map");

        Ok(())
    }

    #[test]
    fn test_hset_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HSet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");
        assert_eq!(result.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }

    #[test]
    fn test_sadd_from_resp_array() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$3\r\nset\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf).unwrap();

        let result: SAdd = frame.try_into().unwrap();
        assert_eq!(result.key, "set");
        assert_eq!(result.members, vec!["hello"]);
    }

    #[test]
    fn test_sismember_from_resp_array() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$9\r\nsismember\r\n$3\r\nset\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf).unwrap();

        let result: SIsMember = frame.try_into().unwrap();
        assert_eq!(result.key, "set");
        assert_eq!(result.member, "hello");
    }

    #[test]
    fn tset_hmget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$5\r\nhmget\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HMGet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.fields, vec!["hello", "world"]);

        Ok(())
    }

    #[test]
    fn test_hset_hget_hgetall_commands() -> Result<()> {
        let backend = crate::Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HSet {
            key: "map".to_string(),
            field: "hello1".to_string(),
            value: RespFrame::BulkString(b"world1".into()),
        };
        cmd.execute(&backend);

        let cmd = HGet {
            key: "map".to_string(),
            field: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"world".into()));

        let cmd = HGetAll {
            key: "map".to_string(),
            sort: true,
        };
        let result = cmd.execute(&backend);

        let expected = RespArray::new([
            BulkString::from("hello").into(),
            BulkString::from("world").into(),
            BulkString::from("hello1").into(),
            BulkString::from("world1").into(),
        ]);
        assert_eq!(result, expected.into());
        Ok(())
    }

    #[test]
    fn test_hset_hmget_commands() -> Result<()> {
        let backend = crate::Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HSet {
            key: "map".to_string(),
            field: "hello1".to_string(),
            value: RespFrame::BulkString(b"world1".into()),
        };
        cmd.execute(&backend);

        let cmd = HMGet {
            key: "map".to_string(),
            fields: vec!["hello".to_string(), "hello1".to_string()],
        };
        let result = cmd.execute(&backend);

        let expected = RespArray::new([
            RespFrame::BulkString(b"world".into()),
            RespFrame::BulkString(b"world1".into()),
        ]);
        assert_eq!(result, expected.into());
        Ok(())
    }

    #[test]
    fn test_sadd_commands() {
        let backend = crate::Backend::new();
        let cmd = SAdd {
            key: "set".to_string(),
            members: vec!["hello".to_string()],
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, 1.into());

        let cmd = SAdd {
            key: "set".to_string(),
            members: vec!["world".to_string()],
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, 1.into());

        let cmd = SAdd {
            key: "set".to_string(),
            members: vec!["hello".to_string(), "world".to_string()],
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, 0.into());
    }

    #[test]
    fn test_sismember_commands() {
        let backend = crate::Backend::new();
        let cmd = SAdd {
            key: "set".to_string(),
            members: vec!["hello".to_string()],
        };
        cmd.execute(&backend);

        let cmd = SIsMember {
            key: "set".to_string(),
            member: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, 1.into());

        let cmd = SIsMember {
            key: "set".to_string(),
            member: "world".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, 0.into());
    }
}
