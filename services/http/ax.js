// services/http/ax.js
import axios from 'axios';
import http from 'http';
import https from 'https';

const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 128 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 128 });

export const ax = axios.create({
  timeout: 15000,              // igual ao que você já usa nos senders
  httpAgent,
  httpsAgent,
});
