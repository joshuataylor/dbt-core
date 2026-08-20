import { Link } from '@dbt-labs/sourdough';

import { Button } from '../../components/ui/Button';

interface Props {
  selector: string;
  label: string;
}

export function SelectorLink({ selector, label }: Props) {
  return (
    <Link isInternal to={`?select=${encodeURIComponent(selector)}`}>
      <Button variant="outline" className="mt-6" text={label} />
    </Link>
  );
}
