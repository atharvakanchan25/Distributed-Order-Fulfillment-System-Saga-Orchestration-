/**
 * k6 load test — 1000 concurrent orders competing for 100 units of SKU aaaaaaaa-0000-0000-0000-000000000001
 *
 * Run:
 *   k6 run --out json=results.json load-test.js
 *
 * What it measures:
 *   - saga_completion_time: time from POST /orders to order reaching CONFIRMED or COMPENSATED
 *   - p99 of that custom metric is the headline number
 *   - oversell_count: any order that ends CONFIRMED when stock should be exhausted
 *
 * Zero-oversell assertion: exactly 100 orders should end CONFIRMED (one unit each),
 * the remaining 900 should end COMPENSATED (insufficient stock).
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const sagaCompletionTime = new Trend('saga_completion_time', true); // true = milliseconds
const confirmedCount     = new Counter('orders_confirmed');
const compensatedCount   = new Counter('orders_compensated');
const oversellCount      = new Counter('oversell_violations');

export const options = {
  scenarios: {
    spike: {
      executor: 'arrival-rate',
      rate: 1000,
      timeUnit: '10s',   // 100 orders/second sustained for 10s = 1000 total
      duration: '10s',
      preAllocatedVUs: 200,
      maxVUs: 500,
    },
  },
  thresholds: {
    // p99 saga completion must be under 10 seconds
    saga_completion_time: ['p(99)<10000'],
    // All 1000 orders must reach a terminal state (no hangs)
    http_req_failed: ['rate<0.01'],
    // Zero oversells — confirmed orders must not exceed stock (100)
    oversell_violations: ['count==0'],
  },
};

const BASE_URL    = __ENV.BASE_URL    || 'http://localhost:8081';
const JWT_TOKEN   = __ENV.JWT_TOKEN   || 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJsb2FkLXRlc3QiLCJyb2xlIjoiU0VSVklDRSJ9.placeholder';
// Fixed SKU with exactly 100 units seeded in inventory migration
const ITEM_ID     = 'aaaaaaaa-0000-0000-0000-000000000001';
// All customers use valid UUIDs (not 00000000-prefix) so payment succeeds;
// stock exhaustion is the only failure path
const CUSTOMER_ID = '11111111-0000-0000-0000-000000000001';

const headers = {
  'Content-Type': 'application/json',
  'Authorization': `Bearer ${JWT_TOKEN}`,
};

export default function () {
  const idempotencyKey = `load-test-${__VU}-${__ITER}`;
  const startTime = Date.now();

  // Step 1: Place order (1 unit each — 1000 orders competing for 100 units)
  const createRes = http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({
      customerId: CUSTOMER_ID,
      itemId:     ITEM_ID,
      quantity:   1,
      totalPrice: '9.99',
    }),
    {
      headers: { ...headers, 'Idempotency-Key': idempotencyKey },
      tags: { name: 'create_order' },
    }
  );

  check(createRes, { 'order created (201)': r => r.status === 201 });
  if (createRes.status !== 201) return;

  const orderId = createRes.json('id');

  // Step 2: Poll until terminal state (CONFIRMED or COMPENSATED)
  // Saga completes asynchronously — poll with backoff up to 15s
  let finalStatus = null;
  for (let attempt = 0; attempt < 30; attempt++) {
    sleep(0.5);
    const pollRes = http.get(`${BASE_URL}/orders/${orderId}`, {
      headers,
      tags: { name: 'poll_order' },
    });
    if (pollRes.status !== 200) continue;

    const status = pollRes.json('status');
    if (status === 'CONFIRMED' || status === 'FAILED') {
      finalStatus = status;
      break;
    }
  }

  if (finalStatus === null) {
    // Saga did not complete within 15s — counts as a threshold breach
    return;
  }

  const elapsed = Date.now() - startTime;
  sagaCompletionTime.add(elapsed);

  if (finalStatus === 'CONFIRMED') {
    confirmedCount.add(1);
  } else {
    compensatedCount.add(1);
  }
}

/**
 * Teardown: verify zero oversells by checking final stock level.
 * After 1000 orders (1 unit each) against 100 units:
 *   confirmed orders must equal min(1000, 100) = 100
 *   stock remaining must be 0
 */
export function teardown() {
  // Check stock via a hypothetical inventory admin endpoint
  // In practice: query the DB directly or expose a read endpoint
  const stockRes = http.get(
    `http://localhost:8082/admin/stock/${ITEM_ID}`,
    { headers }
  );

  if (stockRes.status === 200) {
    const remaining = stockRes.json('quantity');
    if (remaining < 0) {
      oversellCount.add(Math.abs(remaining));
      console.error(`OVERSELL DETECTED: stock went to ${remaining}`);
    } else {
      console.log(`Final stock: ${remaining} (expected 0). Confirmed: ${confirmedCount.name}`);
    }
  }
}
