// How the tree marks the three reasons a recorded call has no replayed
// counterpart. The rows are real ledger rows, and each parses equal to a line of
// the ledger the scorer wrote for it:
//
// - 9593 and 9697 from rp-sbx-fde19cb433-fde19cb-09300817-nx-0930100503002.ledger,
//   correlation 01a0f167-bce5-74b0-8467-e3fb090f02c8. 9593 is the uuid drawn
//   inside the served create_merchant_publishable_key, written `omitted` by that
//   run and `nested_in_served_call` now; the literal below is that line with
//   the kind as it is now written and the `served_ancestor` the scorer now adds.
//   9697 is a clock read after the response, `omitted` then and now.
// - 8677 from rp-sbx-fde19cb433-fde19cb-09300822-o6-0930100718896.ledger,
//   correlation 01a0f16a-7bb1-7812-a438-808d9a87eba4: a redis DEL the stop cut
//   off, `pruned_subtree` and blocking.

import { describe, expect, it } from "vitest";
import { CallRecord } from "./api";
import { isFinding, markOf } from "./spine";

const nested: CallRecord = JSON.parse(
  "{\"correlation_id\":\"01a0f167-bce5-74b0-8467-e3fb090f02c8\",\"source_event_global_sequence\":9593,\"boundary\":\"id\",\"trait_name\":\"common_utils\",\"method_name\":\"generate_uuid_v4\",\"kind\":\"nested_in_served_call\",\"blocking\":false,\"recorded\":{\"args\":{},\"result\":\"20113f65-0755-4727-a7f4-2bd7da827847\",\"is_error\":false,\"call_file\":\"crates/router/src/core/admin.rs\",\"call_line\":84,\"call_column\":9,\"span_path\":\"HTTP request>ROOT_SPAN>merchant_account_create>server_wrap>server_wrap_util\",\"graph_node_id\":32719},\"served_ancestor\":{\"global_sequence\":9592,\"boundary\":\"id\",\"method_name\":\"create_merchant_publishable_key\",\"call_file\":\"crates/router/src/core/admin.rs\",\"call_line\":436}}",
);
const afterResponse: CallRecord = JSON.parse(
  "{\"correlation_id\":\"01a0f167-bce5-74b0-8467-e3fb090f02c8\",\"source_event_global_sequence\":9697,\"boundary\":\"time\",\"trait_name\":\"common_utils\",\"method_name\":\"date_time::now_unix_timestamp_millis\",\"kind\":\"omitted\",\"blocking\":false,\"recorded\":{\"args\":{},\"result\":1790756502907,\"is_error\":false,\"call_file\":\"crates/router/src/services/kafka.rs\",\"call_line\":397,\"call_column\":25,\"span_path\":\"HTTP request>ROOT_SPAN>merchant_account_create>server_wrap>server_wrap_util\",\"graph_node_id\":32719}}",
);
const cutOff: CallRecord = JSON.parse(
  "{\"correlation_id\":\"01a0f16a-7bb1-7812-a438-808d9a87eba4\",\"source_event_global_sequence\":8677,\"boundary\":\"redis\",\"trait_name\":\"redis_interface::module::redis_rs::commands\",\"method_name\":\"delete_key\",\"kind\":\"pruned_subtree\",\"blocking\":true,\"recorded\":{\"args\":{\"command\":\"DEL\",\"key\":\"business_profile_pro_n4J2FsUcPSbHVNkSkj5m\"},\"result\":{\"result\":\"Ok\",\"type_name\":\"redis_interface::types::DelReply\",\"value\":\"KeyDeleted\",\"version\":1},\"is_error\":false,\"call_file\":\"crates/redis_interface/src/module/redis_rs/commands.rs\",\"call_line\":554,\"call_column\":9,\"span_path\":\"HTTP request>ROOT_SPAN>profile_update>server_wrap>server_wrap_util>update_profile_by_profile_id>update_profile_by_profile_id>redact_from_redis_and_publish>delete_multiple_keys>delete_key\",\"graph_node_id\":30907}}",
);

describe("markOf for a recorded call with no replayed counterpart", () => {
  it("marks a call inside a served call apart, and not as a finding", () => {
    expect(nested.served_ancestor?.global_sequence).toBe(9592);
    const m = markOf(nested)!;
    expect(m).toBe("inside-served");
    expect(isFinding(m)).toBe(false);
  });

  it("keeps a genuine omission and a pruned subtree as findings", () => {
    for (const c of [afterResponse, cutOff]) {
      const m = markOf(c)!;
      expect(m).toBe("omitted");
      expect(isFinding(m)).toBe(true);
    }
  });

  it("shows a call inside a served call as an omission when the scorer charges it", () => {
    const m = markOf({ ...nested, blocking: true })!;
    expect(m).toBe("omitted");
    expect(isFinding(m)).toBe(true);
  });
});
