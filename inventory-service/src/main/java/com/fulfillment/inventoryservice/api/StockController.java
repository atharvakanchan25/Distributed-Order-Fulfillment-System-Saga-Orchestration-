package com.fulfillment.inventoryservice.api;

import com.fulfillment.inventoryservice.domain.StockItemRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/admin/stock")
@RequiredArgsConstructor
public class StockController {

    private final StockItemRepository stockItemRepository;

    @GetMapping("/{itemId}")
    public ResponseEntity<Map<String, Object>> getStock(@PathVariable UUID itemId) {
        return stockItemRepository.findByItemId(itemId)
                .map(s -> ResponseEntity.ok(Map.<String, Object>of(
                        "itemId",   s.getItemId(),
                        "quantity", s.getQuantity(),
                        "version",  s.getVersion())))
                .orElse(ResponseEntity.notFound().build());
    }
}
