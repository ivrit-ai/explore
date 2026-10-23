import type { SVGProps } from 'react';

type IconProps = SVGProps<SVGSVGElement> & { size?: number };

function stroke({ size = 18, ...rest }: IconProps) {
  return {
    width: size,
    height: size,
    viewBox: '0 0 24 24',
    fill: 'none',
    stroke: 'currentColor',
    strokeWidth: 1.8,
    strokeLinecap: 'round' as const,
    strokeLinejoin: 'round' as const,
    'aria-hidden': true,
    ...rest,
  };
}

function solid({ size = 16, ...rest }: IconProps) {
  return { width: size, height: size, viewBox: '0 0 24 24', fill: 'currentColor', 'aria-hidden': true, ...rest };
}

export const SearchIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <circle cx="11" cy="11" r="7" />
    <path d="M20 20l-3.5-3.5" />
  </svg>
);
export const ArrowLeftIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M19 12H5M11 6l-6 6 6 6" />
  </svg>
);
export const ChevronDownIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M6 9l6 6 6-6" />
  </svg>
);
export const FilterIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M4 7h16M7 12h10M10 17h4" />
  </svg>
);
export const ShuffleIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5" />
  </svg>
);
export const DownloadIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M12 4v11M7 10l5 5 5-5M5 20h14" />
  </svg>
);
export const LinkIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M10 14a4 4 0 0 0 5.66 0l3-3a4 4 0 0 0-5.66-5.66l-1 1" />
    <path d="M14 10a4 4 0 0 0-5.66 0l-3 3a4 4 0 0 0 5.66 5.66l1-1" />
  </svg>
);
export const CopyIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <rect x="9" y="9" width="11" height="11" rx="2" />
    <path d="M5 15V5a1 1 0 0 1 1-1h10" />
  </svg>
);
export const CheckIcon = (p: IconProps) => (
  <svg {...stroke({ strokeWidth: 2.4, ...p })}>
    <path d="M5 12.5l4.5 4.5L19 7.5" />
  </svg>
);
export const CloseIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M6 6l12 12M18 6L6 18" />
  </svg>
);
export const SunIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <circle cx="12" cy="12" r="4" />
    <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
  </svg>
);
export const MoonIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z" />
  </svg>
);
/** Half-filled circle: "follow the system setting". */
export const SystemIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <circle cx="12" cy="12" r="8" />
    <path d="M12 4a8 8 0 0 1 0 16z" fill="currentColor" stroke="none" />
  </svg>
);
export const MenuIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M4 8h16M4 16h16" />
  </svg>
);
export const PlayIcon = (p: IconProps) => (
  <svg {...solid(p)}>
    <path d="M7 4.5v15l12.5-7.5z" />
  </svg>
);
export const PauseIcon = (p: IconProps) => (
  <svg {...solid(p)}>
    <path d="M7 5h3.5v14H7zM13.5 5H17v14h-3.5z" />
  </svg>
);
export const PrevIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M19 5L9 12l10 7z" />
    <path d="M5 5v14" />
  </svg>
);
export const NextIcon = (p: IconProps) => (
  <svg {...stroke(p)}>
    <path d="M5 5l10 7-10 7z" />
    <path d="M19 5v14" />
  </svg>
);
