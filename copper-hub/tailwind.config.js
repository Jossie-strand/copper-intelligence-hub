/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        copper: {
          DEFAULT: '#C87941',
          light: '#E8A76C',
          dim: '#8B5A2B',
        },
        bg: {
          primary: '#080D14',
          card: '#0C1220',
        },
        border: '#1A2332',
        txt: {
          primary: '#E8E4DC',
          secondary: '#9CA3AF',
        },
        signal: {
          bull: '#22C55E',
          bear: '#EF4444',
          neutral: '#F59E0B',
        },
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'monospace'],
        display: ['Syne', 'sans-serif'],
        body: ['Inter', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
