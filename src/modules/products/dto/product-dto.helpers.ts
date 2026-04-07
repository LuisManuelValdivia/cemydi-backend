type TransformInput = {
  value: unknown;
};

function toTrimmedString(value: unknown) {
  return typeof value === 'string' ? value.trim() : '';
}

export function transformBoolean({ value }: TransformInput) {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'si', 'yes', 'y'].includes(normalized)) {
      return true;
    }
    if (['false', '0', 'no', 'n'].includes(normalized)) {
      return false;
    }
  }

  return value;
}

export function transformNumber({ value }: TransformInput) {
  if (typeof value === 'number') {
    return value;
  }

  if (typeof value === 'string') {
    const normalized = value.trim();
    if (!normalized) {
      return value;
    }

    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : value;
  }

  return value;
}

export function transformInteger({ value }: TransformInput) {
  const parsed = transformNumber({ value });
  if (typeof parsed === 'number' && Number.isInteger(parsed)) {
    return parsed;
  }

  return value;
}

function normalizeArray(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }

    if (trimmed.startsWith('[')) {
      try {
        const parsed: unknown = JSON.parse(trimmed);
        return Array.isArray(parsed) ? parsed : [trimmed];
      } catch {
        return [trimmed];
      }
    }

    return [trimmed];
  }

  return value === undefined || value === null ? [] : [value];
}

export function transformStringArray({ value }: TransformInput): string[] {
  const candidates = normalizeArray(value);
  return candidates
    .flatMap((candidate) => {
      if (typeof candidate !== 'string') {
        return [candidate];
      }

      return candidate.includes('\n')
        ? candidate.split(/\r?\n/g)
        : candidate.includes('|')
          ? candidate.split('|')
          : [candidate];
    })
    .map((candidate) => toTrimmedString(candidate))
    .filter(Boolean);
}

export function transformIntegerArray({
  value,
}: TransformInput): Array<number | string> {
  return normalizeArray(value)
    .map((candidate) => {
      if (typeof candidate === 'number' && Number.isInteger(candidate)) {
        return candidate;
      }

      if (typeof candidate === 'string') {
        const trimmed = candidate.trim();
        if (!trimmed) {
          return null;
        }

        const parsed = Number(trimmed);
        return Number.isInteger(parsed) ? parsed : candidate;
      }

      return typeof candidate === 'string' || typeof candidate === 'number'
        ? candidate
        : null;
    })
    .filter((candidate): candidate is number | string => candidate !== null);
}
