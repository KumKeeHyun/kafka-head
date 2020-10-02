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
func (offsetKey *OffsetCommitKey) Parse(buf *bytes.Buffer) (string, error) {
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

func (offsetValue *OffsetCommitValue) Parse(buf *bytes.Buffer, version int16) (string, error) {
	switch version {
	case 1:
		return offsetValue.ParseV1(buf)
	case 2:
		return offsetValue.ParseV2(buf)
	case 3:
		return offsetValue.ParseV3(buf)
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
func (offsetValue *OffsetCommitValue) ParseV1(buf *bytes.Buffer) (string, error) {
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
func (offsetValue *OffsetCommitValue) ParseV2(buf *bytes.Buffer) (string, error) {
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
func (offsetValue *OffsetCommitValue) ParseV3(buf *bytes.Buffer) (string, error) {
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

// kafka definition
// val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]
func (metaKey *GroupMetadataKey) Parse(buf *bytes.Buffer) (string, error) {
	var err error

	if metaKey.Group, err = readString(buf); err != nil {
		return "group", err
	}
	return "", nil
}

// kafka definition
// value.set(PROTOCOL_TYPE_KEY, groupMetadata.protocolType.getOrElse(""))
// value.set(GENERATION_KEY, groupMetadata.generationId)
// value.set(PROTOCOL_KEY, groupMetadata.protocolName.orNull)
// value.set(LEADER_KEY, groupMetadata.leaderOrNull)
// if (version >= 2)
//   value.set(CURRENT_STATE_TIMESTAMP_KEY, groupMetadata.currentStateTimestampOrDefault)
func (metaHeader *GroupMetadataHeader) Parse(buf *bytes.Buffer, version int16) (string, error) {
	var err error

	if metaHeader.ProtocolType, err = readString(buf); err != nil {
		return "protocolType", err
	}
	if err = binary.Read(buf, binary.BigEndian, &metaHeader.Generation); err != nil {
		return "Generation", err
	}
	if metaHeader.Protocol, err = readString(buf); err != nil {
		return "Protocol", err
	}
	if metaHeader.Leader, err = readString(buf); err != nil {
		return "leader", err
	}
	if version >= 2 {
		if err = binary.Read(buf, binary.BigEndian, &metaHeader.CurrentStateTimestamp); err != nil {
			return "currentStateTimestamp", err
		}
	}
	return "", nil
}

// kafka definition
// memberStruct.set(MEMBER_ID_KEY, memberMetadata.memberId)
// if (version >= 3)
//   원래는 GROUP_INSTANCE_ID_KEY가 REBALANCE_TIMEOUT_KEY 이후에 Set되지만 byte를 읽으면 이순서로 읽힘..
// 	 memberStruct.set(GROUP_INSTANCE_ID_KEY, memberMetadata.groupInstanceId.orNull)
// memberStruct.set(CLIENT_ID_KEY, memberMetadata.clientId)
// memberStruct.set(CLIENT_HOST_KEY, memberMetadata.clientHost)
// memberStruct.set(SESSION_TIMEOUT_KEY, memberMetadata.sessionTimeoutMs)
// if (version > 0)
// 	 memberStruct.set(REBALANCE_TIMEOUT_KEY, memberMetadata.rebalanceTimeoutMs)
// memberStruct.set(SUBSCRIPTION_KEY, ByteBuffer.wrap(metadata))
// memberStruct.set(ASSIGNMENT_KEY, ByteBuffer.wrap(memberAssignment))
func (member *MemberMetadata) Parse(buf *bytes.Buffer, version int16) (string, error) {
	var (
		err                                         error
		subscriptionLen, assignmentLen, userDataLen int32
		numOfTopics, numOfPartitions, partitionID   int32
		consumerProtocolVersion                     int16
		topicName                                   string
	)

	if member.MemberID, err = readString(buf); err != nil {
		return "memberID", err
	}
	if version >= 3 {
		if member.GroupInstanceID, err = readString(buf); err != nil {
			return "groupInstanceID", err
		}
	}
	if member.ClientID, err = readString(buf); err != nil {
		return "clientID", err
	}
	//fmt.Println(buf.Bytes())
	if member.ClientHost, err = readString(buf); err != nil {
		return "clientHost", err
	}
	//fmt.Println(buf.Bytes())
	if err = binary.Read(buf, binary.BigEndian, &member.SessionTimeout); err != nil {
		return "sessionTimeout", err
	}
	if version > 0 {
		if err = binary.Read(buf, binary.BigEndian, &member.RebalanceTimeout); err != nil {
			return "rebalanceTimeout", err
		}
	}

	// skip subscription (topicName array)
	if err = binary.Read(buf, binary.BigEndian, &subscriptionLen); err != nil {
		return "subscription", err
	} else if subscriptionLen > 0 {
		buf.Next(int(subscriptionLen))
	}

	if err = binary.Read(buf, binary.BigEndian, &assignmentLen); err != nil {
		return "assignment", err
	} else if assignmentLen > 0 {
		member.Assignment = make(map[string][]int32)
		if err = binary.Read(buf, binary.BigEndian, &consumerProtocolVersion); err != nil {
			return "assignment-consumerProtocolVersion", err
		}
		if err = binary.Read(buf, binary.BigEndian, &numOfTopics); err != nil {
			return "assignment-numOfTopics", err
		}
		for i := 0; i < int(numOfTopics); i++ {
			if topicName, err = readString(buf); err != nil {
				return "assignment-topicName", err
			}
			if err = binary.Read(buf, binary.BigEndian, &numOfPartitions); err != nil {
				return "assignment-numOfPartitions", err
			}
			partitionIDs := make([]int32, numOfPartitions)
			for j := 0; j < int(numOfPartitions); j++ {
				if err = binary.Read(buf, binary.BigEndian, &partitionID); err != nil {
					return "assignment-partitionID", err
				}
				partitionIDs[j] = partitionID
			}
			member.Assignment[topicName] = partitionIDs
		}

		// skip userData
		if err = binary.Read(buf, binary.BigEndian, &userDataLen); err != nil {
			return "userDataLen", err
		} else if userDataLen > 0 {
			buf.Next(int(userDataLen))
		}
	}

	return "", nil
}
