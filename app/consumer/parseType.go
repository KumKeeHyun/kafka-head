package consumer

// reference 1
// apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala
// GroupMetadataManager (Line : 995 ~)

// reference 2
// https://kafka.apache.org/protocol#protocol_types

// ---------- Kafka Protocol Primitive Types ----------
// TYPE : INT(N)
// DESCRIPTION : big-endian

// TYPE : STRING
// DESCRIPTION : first, length N (INT16). Then N bytes UTF-8 character

// TYPE : NULLABLE_STRING
// DESCRIPTION : if null string, length -1 (INT16), Then no following bytes. Otherwise, same with STRING

// TYPE : ARRAY
// DESCRIPTION : first, length N (INT32). Then N instance of type follow.

// TYPE : BYTES
// DESCRIPTION : first, lenght N (INT32). Then N bytes follow.
// ----------------------------------------------------

// key version kafka definition
// private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
// private val CURRENT_GROUP_KEY_SCHEMA_VERSION = 2.toShort
var (
	OffsetCommitVersion  int16 = 1
	GroupMetadataVersion int16 = 2
)

// ---------------------------------------------------------------------
// offset commit
// ---------------------------------------------------------------------

// OffsetCommitKey kafka definition
// private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
//     new Field("topic", STRING),
//     new Field("partition", INT32))
type OffsetCommitKey struct {
	Group     string
	Topic     string
	Partition int32
}

// OffsetCommitValue all in one
type OffsetCommitValue struct {
	Offset          int64
	LeaderEpoch     int32
	Metadata        string
	CommitTimestamp int64
	ExpireTimestamp int64
}

// OffsetCommitValueV0 kafka definition
// private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
//     new Field("metadata", STRING, "Associated metadata.", ""),
//     new Field("timestamp", INT64))
type OffsetCommitValueV0 struct {
	Offset    int64
	Metadata  string
	Timestamp int64
}

// OffsetCommitValueV1 kafka definition
// private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
//     new Field("metadata", STRING, "Associated metadata.", ""),
//     new Field("commit_timestamp", INT64),
//     new Field("expire_timestamp", INT64))
type OffsetCommitValueV1 struct {
	Offset          int64
	Metadata        string
	CommitTimestamp int64
	ExpireTimestamp int64
}

// OffsetCommitValueV2 kafka definition
// private val OFFSET_COMMIT_VALUE_SCHEMA_V2 = new Schema(new Field("offset", INT64),
//     new Field("metadata", STRING, "Associated metadata.", ""),
//     new Field("commit_timestamp", INT64))
type OffsetCommitValueV2 struct {
	Offset          int64
	Metadata        string
	CommitTimestamp int64
}

// OffsetCommitValueV3 kafka definition
// private val OFFSET_COMMIT_VALUE_SCHEMA_V3 = new Schema(
//     new Field("offset", INT64),
//     new Field("leader_epoch", INT32),
//     new Field("metadata", STRING, "Associated metadata.", ""),
//     new Field("commit_timestamp", INT64))
type OffsetCommitValueV3 struct {
	Offset          int64
	LeaderEpoch     int32
	Metadata        string
	CommitTimestamp int64
}

// ---------------------------------------------------------------------
// group metadata
// ---------------------------------------------------------------------

// GroupMetadataKey kafka definition
// private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
type GroupMetadataKey struct {
	Group string
}

// GroupMetadataHeader all in one
type GroupMetadataHeader struct {
	ProtocolType          string
	Generation            int32
	Protocol              string
	Leader                string
	CurrentStateTimestamp int64
}

// GroupMetadataHeaderV0 kafka definition
// private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
//     new Field(PROTOCOL_TYPE_KEY, STRING),
//     new Field(GENERATION_KEY, INT32),
//     new Field(PROTOCOL_KEY, NULLABLE_STRING),
//     new Field(LEADER_KEY, NULLABLE_STRING),
//     new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V0)))
type GroupMetadataHeaderV0 struct {
	ProtocolType string
	Generation   int32
	Protocol     string
	Leader       string
}

// GroupMetadataHeaderV1 is same with GroupMetadataHeaderV0
type GroupMetadataHeaderV1 GroupMetadataHeaderV0

// GroupMetadataHeaderV2 kafka definition
// private val GROUP_METADATA_VALUE_SCHEMA_V2 = new Schema(
//     new Field(PROTOCOL_TYPE_KEY, STRING),
//     new Field(GENERATION_KEY, INT32),
//     new Field(PROTOCOL_KEY, NULLABLE_STRING),
//     new Field(LEADER_KEY, NULLABLE_STRING),
//     new Field(CURRENT_STATE_TIMESTAMP_KEY, INT64),
//     new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V2)))
type GroupMetadataHeaderV2 struct {
	ProtocolType          string
	Generation            int32
	Protocol              string
	Leader                string
	CurrentStateTimestamp int64
}

// GroupMetadataHeaderV3 is same with GroupMetadataHeaderV2
type GroupMetadataHeaderV3 GroupMetadataHeaderV2

// MemberMetadata all in one
type MemberMetadata struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	SessionTimeout   int32
	RebalanceTimeout int32
	GroupInstanceID  string
	// Subscription []string
	Assignment map[string][]int32 // topics-partitions data
}

// MemberMetadataV0 kafka definition
// private val MEMBER_METADATA_V0 = new Schema(
//     new Field(MEMBER_ID_KEY, STRING),
//     new Field(CLIENT_ID_KEY, STRING),
//     new Field(CLIENT_HOST_KEY, STRING),
//     new Field(SESSION_TIMEOUT_KEY, INT32),
//     new Field(SUBSCRIPTION_KEY, BYTES),
//     new Field(ASSIGNMENT_KEY, BYTES))
type MemberMetadataV0 struct {
	MemberID       string
	ClientID       string
	ClientHost     string
	SessionTimeout int32
	Assignment     map[string][]int32
}

// MemberMetadataV1 kafka definition
// private val MEMBER_METADATA_V1 = new Schema(
//     new Field(MEMBER_ID_KEY, STRING),
//     new Field(CLIENT_ID_KEY, STRING),
//     new Field(CLIENT_HOST_KEY, STRING),
//     new Field(REBALANCE_TIMEOUT_KEY, INT32),
//     new Field(SESSION_TIMEOUT_KEY, INT32),
//     new Field(SUBSCRIPTION_KEY, BYTES),
//     new Field(ASSIGNMENT_KEY, BYTES))
type MemberMetadataV1 struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	SessionTimeout   int32
	RebalanceTimeout int32
	Assignment       map[string][]int32
}

// MemberMetadataV2 kafka definition
type MemberMetadataV2 MemberMetadataV1

// MemberMetadataV3 kafka definition
// private val MEMBER_METADATA_V3 = new Schema(
//     new Field(MEMBER_ID_KEY, STRING),
//     new Field(GROUP_INSTANCE_ID_KEY, NULLABLE_STRING),
//     new Field(CLIENT_ID_KEY, STRING),
//     new Field(CLIENT_HOST_KEY, STRING),
//     new Field(REBALANCE_TIMEOUT_KEY, INT32),
//     new Field(SESSION_TIMEOUT_KEY, INT32),
//     new Field(SUBSCRIPTION_KEY, BYTES),
//     new Field(ASSIGNMENT_KEY, BYTES))
type MemberMetadataV3 struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	SessionTimeout   int32
	RebalanceTimeout int32
	GroupInstanceID  string
	Assignment       map[string][]int32
}
