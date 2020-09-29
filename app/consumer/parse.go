package consumer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// burrow copy
func readString(buf *bytes.Buffer) (string, error) {
	var strlen int16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	if strlen == -1 {
		return "", nil
	}

	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

// kafka definition
// key.set(OFFSET_KEY_GROUP_FIELD, groupId)
// key.set(OFFSET_KEY_TOPIC_FIELD, topicPartition.topic)
// key.set(OFFSET_KEY_PARTITION_FIELD, topicPartition.partition)
func (offsetKey *OffsetCommitKey) parse(buf *bytes.Buffer) (string, error) {
	var err error

	if offsetKey.Group, err = readString(buf); err != nil {
		return "group", err
	}
	if offsetKey.Topic, err = readString(buf); err != nil {
		return "topic", err
	}
	if err = binary.Read(buf, binary.BigEndian, &offsetKey.Partition); err != nil {
		return "partition", err
	}
	return "", nil
}

func (offsetValue *OffsetCommitValue) parse(buf *bytes.Buffer, version int16) (string, error) {
	switch version {
	case 1:
		return offsetValue.parseV1(buf)
	case 2:
		return offsetValue.parseV2(buf)
	case 3:
		return offsetValue.parseV3(buf)
	default:
		return "version", fmt.Errorf("invalid offsetCommitValue version")
	}
}

// kafka definition
// value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
// value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
// value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
// value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1,
//     offsetAndMetadata.expireTimestamp.getOrElse(OffsetCommitRequest.DEFAULT_TIMESTAMP))
func (offsetValue *OffsetCommitValue) parseV1(buf *bytes.Buffer) (string, error) {
	var err error

	if err = binary.Read(buf, binary.BigEndian, &offsetValue.Offset); err != nil {
		return "offset", err
	}
	if offsetValue.Metadata, err = readString(buf); err != nil {
		return "metadata", err
	}
	if err = binary.Read(buf, binary.BigEndian, &offsetValue.CommitTimestamp); err != nil {
		return "commitTimestamp", err
	}
	if err = binary.Read(buf, binary.BigEndian, &offsetValue.ExpireTimestamp); err != nil {
		return "expireTimestamp", err
	}
	return "", nil
}

// kafka definition
// value.set(OFFSET_VALUE_OFFSET_FIELD_V2, offsetAndMetadata.offset)
// value.set(OFFSET_VALUE_METADATA_FIELD_V2, offsetAndMetadata.metadata)
// value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2, offsetAndMetadata.commitTimestamp)
func (offsetValue *OffsetCommitValue) parseV2(buf *bytes.Buffer) (string, error) {
	var err error

	if err = binary.Read(buf, binary.BigEndian, &offsetValue.Offset); err != nil {
		return "offset", err
	}
	if offsetValue.Metadata, err = readString(buf); err != nil {
		return "metadata", err
	}
	if err = binary.Read(buf, binary.BigEndian, &offsetValue.CommitTimestamp); err != nil {
		return "commitTimestamp", err
	}
	return "", nil
}

// kafka definition
// value.set(OFFSET_VALUE_OFFSET_FIELD_V3, offsetAndMetadata.offset)
// value.set(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3,
//     offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
// value.set(OFFSET_VALUE_METADATA_FIELD_V3, offsetAndMetadata.metadata)
// value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3, offsetAndMetadata.commitTimestamp)
func (offsetValue *OffsetCommitValue) parseV3(buf *bytes.Buffer) (string, error) {
	var err error

	if err = binary.Read(buf, binary.BigEndian, &offsetValue.Offset); err != nil {
		return "offset", err
	}
	if err = binary.Read(buf, binary.BigEndian, &offsetValue.LeaderEpoch); err != nil {
		return "leaderEpoch", err
	}
	if offsetValue.Metadata, err = readString(buf); err != nil {
		return "metadata", err
	}
	if err = binary.Read(buf, binary.BigEndian, &offsetValue.CommitTimestamp); err != nil {
		return "commitTimestamp", err
	}
	return "", nil
}
