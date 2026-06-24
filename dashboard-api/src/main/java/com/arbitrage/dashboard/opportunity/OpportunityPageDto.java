package com.arbitrage.dashboard.opportunity;

import java.util.List;

/**
 * Paginated response envelope for the historical opportunities list endpoint.
 *
 * @param opportunities  the current page of closed/expired opportunities
 * @param totalCount     total number of rows matching the current filter (for page count display)
 * @param page           current page index (0-based)
 * @param size           requested page size
 */
public record OpportunityPageDto(
        List<ClosedOpportunityDto> opportunities,
        long totalCount,
        int page,
        int size
) {}
