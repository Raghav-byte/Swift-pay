package com.swiftpay.analytics.controller;

import com.swiftpay.analytics.dto.AnalyticsSummaryDTO;
import com.swiftpay.analytics.dto.VolumeByMinuteDTO;
import com.swiftpay.analytics.exception.SwiftPayException;
import com.swiftpay.analytics.service.AnalyticsQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/analytics")
public class AnalyticsController {

    private final AnalyticsQueryService analyticsQueryService;

    public AnalyticsController(AnalyticsQueryService analyticsQueryService) {
        this.analyticsQueryService = analyticsQueryService;
    }

    @Operation(
            summary = "Aggregate transaction summary by currency",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Summary returned"),
                    @ApiResponse(responseCode = "500", description = "Query failed")
            })
    @GetMapping("/summary")
    public ResponseEntity<AnalyticsSummaryDTO> getSummary(
            @RequestParam(defaultValue = "INR") String currency) throws SwiftPayException {
        return ResponseEntity.ok(analyticsQueryService.getSummary(currency));
    }

    @Operation(
            summary = "Transaction volume grouped by minute",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Volume series returned"),
                    @ApiResponse(responseCode = "400", description = "Invalid minutes parameter"),
                    @ApiResponse(responseCode = "500", description = "Query failed")
            })
    @GetMapping("/volume")
    public ResponseEntity<List<VolumeByMinuteDTO>> getVolume(
            @RequestParam(defaultValue = "60") int minutes) throws SwiftPayException {
        return ResponseEntity.ok(analyticsQueryService.getVolumeByMinute(minutes));
    }

}
