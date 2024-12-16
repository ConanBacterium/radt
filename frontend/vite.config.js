import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');

  return {
    plugins: [
      react()
    ],
    optimizeDeps: {
      exclude: ['@duckdb/duckdb-wasm']
    },
    server: {
      host: true, // Needed for Docker
      port: 4000,
      headers: {
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp',
        'Cross-Origin-Resource-Policy': 'cross-origin'
      },
      proxy: {  // Proxy goes here, inside an object
        '/api': {
          target: env.VITE_API_URL || 'http://localhost:8000',
          changeOrigin: true,
          secure: false,
          // rewrite: (path) => path.replace(/^\/api/, '')
        }
      }
    },
    worker: {
      format: 'es'
    },
    resolve: {
      extensions: ['.js', '.jsx']  // Add this to handle both .js and .jsx
    }
  }
});