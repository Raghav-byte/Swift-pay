import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Pre-seeded owner IDs from docs/seed.sql (run in Supabase before the test)
const SENDER_IDS = [
  'b0000001-0000-0000-0000-000000000001',
  'b0000002-0000-0000-0000-000000000002',
  'b0000003-0000-0000-0000-000000000003',
  'b0000004-0000-0000-0000-000000000004',
  'b0000005-0000-0000-0000-000000000005',
];

const RECEIVER_IDS = [
  'b0000006-0000-0000-0000-000000000006',
  'b0000007-0000-0000-0000-000000000007',
  'b0000008-0000-0000-0000-000000000008',
  'b0000009-0000-0000-0000-000000000009',
  'b0000010-0000-0000-0000-000000000010',
];

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const RATE = Number(__ENV.K6_RATE) || 250;
const TARGET_REQUESTS = Number(__ENV.K6_TARGET_REQUESTS) || 0;
const duration =
  TARGET_REQUESTS > 0
    ? `${Math.ceil(TARGET_REQUESTS / RATE)}s`
    : __ENV.K6_DURATION || '72m';

export const options = {
  scenarios: {
    constant_rate: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: Number(__ENV.K6_PRE_ALLOCATED_VUS) || 300,
      maxVUs: Number(__ENV.K6_MAX_VUS) || 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

export default function () {
  const senderId = SENDER_IDS[Math.floor(Math.random() * SENDER_IDS.length)];
  let receiverId = RECEIVER_IDS[Math.floor(Math.random() * RECEIVER_IDS.length)];
  while (receiverId === senderId) {
    receiverId = RECEIVER_IDS[Math.floor(Math.random() * RECEIVER_IDS.length)];
  }

  const payload = JSON.stringify({
    transactionId: uuidv4(),
    senderId,
    receiverId,
    amount: (Math.random() * 100 + 1).toFixed(2),
    currency: 'INR',
  });

  const res = http.post(`${BASE_URL}/v1/payments`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202': (r) => r.status === 202,
    'has transactionId': (r) => {
      try {
        return JSON.parse(r.body).transactionId !== undefined;
      } catch {
        return false;
      }
    },
  });
}
