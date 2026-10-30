// Tailwind CSS 4 runs as a single PostCSS plugin. It adds vendor prefixes itself, so autoprefixer
// is not used, and the widgets rely on the default palette only, so no theme options belong here.
const config = {
  plugins: {
    "@tailwindcss/postcss": {},
  },
};

export default config;
