import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Support both local dev (localhost:8000) and Docker compose mode (api:8000)
// Set VITE_API_TARGET / VITE_WS_TARGET in docker-compose.yml to override
const apiTarget = process.env.VITE_API_TARGET || 'http://localhost:8000'
const wsTarget  = process.env.VITE_WS_TARGET  || 'ws://localhost:8000'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: apiTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
      '/ws': {
        target: wsTarget,
        ws: true,
        changeOrigin: true,
      },
    },
  },
})
