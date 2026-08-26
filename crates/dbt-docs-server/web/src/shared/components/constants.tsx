import { BadgeAlert, BadgeCheck, BadgeMinus } from 'lucide-react';

import { SizeType } from '@dbt-labs/sourdough';

import { TrustState } from '../util/trustSignals';

/** Mirrors sourdough's rendered ryecon sizes (Ryecons.css custom properties). */
const SIZE_CLASS: Record<SizeType, string> = {
  xs: 'size-3',
  sm: 'size-3.5',
  md: 'size-4',
  lg: 'size-5',
  xl: 'size-6',
  '2xl': 'size-8',
  '3xl': 'size-10',
  '4xl': 'size-12',
};

export const trustIconMap = (
  size: SizeType,
): Record<TrustState, React.ReactElement> => {
  const sizeClass = SIZE_CLASS[size];
  return {
    healthy: (
      <BadgeCheck data-testid="healthy-icon" className={`mt-[2px] ${sizeClass}`} />
    ),
    caution: (
      <BadgeAlert data-testid="caution-icon" className={`mt-[2px] ${sizeClass}`} />
    ),
    degraded: (
      <BadgeAlert data-testid="degraded-icon" className={`mt-[2px] ${sizeClass}`} />
    ),
    unknown: (
      <BadgeMinus data-testid="unknown-icon" className={`mt-[2px] ${sizeClass}`} />
    ),
  };
};
