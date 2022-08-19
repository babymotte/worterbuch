pub mod error;

pub use worterbuch_codec::*;

#[macro_export]
macro_rules! topic {
    ( $sep:expr, $( $x:expr ),+ ) => {
        {
            let mut segments = Vec::new();
            $(
                segments.push($x.to_string());
            )+
            segments.join(&$sep.to_string())
        }
    };
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn single_segment_topic_generates_correctly() {
        assert_eq!("topic", topic!('/', "topic"));
    }

    #[test]
    fn multi_segment_topic_generates_correctly() {
        assert_eq!("some/test/topic", topic!('/', "some", "test", "topic"));
    }

    #[test]
    fn wildcard_topic_generates_correctly() {
        assert_eq!("some/?/topic/#", topic!('/', "some", '?', "topic", '#'));
    }

    #[test]
    fn topic_with_postfix_generates_correctly() {
        let sep = '_';
        let postfix = topic!(sep, "some", "predefined", "postfix");
        assert_eq!(
            "topic_with_some_predefined_postfix",
            topic!(sep, "topic", "with", postfix)
        );
    }

    #[test]
    fn topic_with_prefix_generates_correctly() {
        let sep = '_';
        let prefix = topic!(sep, "some", "predefined", "prefix");
        assert_eq!(
            "some_predefined_prefix_with_topic",
            topic!(sep, prefix, "with", "topic")
        );
    }
}
