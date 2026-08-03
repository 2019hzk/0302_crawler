/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{vue,js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        display: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
        body: ['"DM Sans"', '"Noto Sans SC"', 'sans-serif'],
      },
      colors: {
        terminal: {
          bg: '#0a0a0b',
          panel: '#121215',
          border: '#1e1e24',
          green: '#00e673',
          amber: '#f59e0b',
          blue: '#3b82f6',
          red: '#ef4444',
          muted: '#6b7280',
          text: '#e5e5e5',
        },
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'slide-up': 'slideUp 0.3s ease-out',
        'fade-in': 'fadeIn 0.4s ease-out',
      },
      keyframes: {
        slideUp: {
          '0%': { transform: 'translateY(8px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
      },
    },
  },
  plugins: [],
}
