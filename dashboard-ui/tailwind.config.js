/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        surface: {
          DEFAULT: '#0f1117',
          raised: '#1a1d27',
          border: '#2a2d3a',
        },
        accent: {
          green: '#22c55e',
          red: '#ef4444',
          yellow: '#eab308',
          blue: '#3b82f6',
        },
      },
    },
  },
  plugins: [],
}

