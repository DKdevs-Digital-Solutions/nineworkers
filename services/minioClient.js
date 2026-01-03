import { Client } from 'minio'
import dotenv from 'dotenv'

dotenv.config()

const endpoint = process.env.MINIO_ENDPOINT.replace(/^https?:\/\//, '')
const port = 443
const useSSL = true

export const minioClient = new Client({
  endPoint: endpoint,
  port,
  useSSL,
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY
})
