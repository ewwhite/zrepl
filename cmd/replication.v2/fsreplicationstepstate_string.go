// Code generated by "stringer -type=FSReplicationStepState"; DO NOT EDIT.

package replication

import "strconv"

const _FSReplicationStepState_name = "StepPendingStepRetryStepPermanentErrorStepCompleted"

var _FSReplicationStepState_index = [...]uint8{0, 11, 20, 38, 51}

func (i FSReplicationStepState) String() string {
	if i < 0 || i >= FSReplicationStepState(len(_FSReplicationStepState_index)-1) {
		return "FSReplicationStepState(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _FSReplicationStepState_name[_FSReplicationStepState_index[i]:_FSReplicationStepState_index[i+1]]
}
