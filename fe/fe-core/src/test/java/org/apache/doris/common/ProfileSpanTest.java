package org.apache.doris.common;

import org.apache.doris.common.profile.ProfileSpan;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.TimeStats;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

class ProfileSpanTest {

    @Test
    void test() throws InterruptedException {
        String nodeId = "id1";
        SummaryProfile sp = new SummaryProfile();
        sp.createNodeTimer("id1");
        try (ProfileSpan profileSpan = ProfileSpan.create(sp, nodeId, "testStep1")) {
            assertNotNull(profileSpan);
        }
        Thread.sleep(1000);
        try (ProfileSpan profileSpan = ProfileSpan.create(sp, nodeId, "testStep2")) {
            assertNotNull(profileSpan);
        }
        Map<String, TimeStats> timerMap = sp.getNodeTimer(nodeId).getScanNodesStats();
        assertEquals(2, timerMap.size());
    }

    @Test
    void testNull() throws InterruptedException {
        String nodeId = "id1";
        SummaryProfile sp = new SummaryProfile();
        sp.createNodeTimer("id1");
        try (ProfileSpan profileSpan = ProfileSpan.create(null, nodeId, "testStep1")) {
            assertEquals(ProfileSpan.EMPTY_PROFILE_SPAN, profileSpan);
        }
    }
}
