import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  // VERY IMPORTANT: Change "kafka-simulator" if you name your GitHub repo something else!
  base: '/kafka-simulator/', 
})