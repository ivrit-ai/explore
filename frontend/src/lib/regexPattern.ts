export interface PatternPiece {
  text: string;
  /** Capture group number, 0 outside any group, -1 for group parentheses. */
  group: number;
}

/**
 * Split a Python regex into runs coloured by the capture group they belong to.
 * Display only: it understands escapes, character classes and group syntax,
 * not the full grammar.
 */
export function patternPieces(pattern: string): { pieces: PatternPiece[]; groupCount: number } {
  const chars = Array.from(pattern);
  const pieces: PatternPiece[] = [];
  const stack: number[] = [];
  let nextGroup = 1;
  const current = () => (stack.length ? stack[stack.length - 1] : 0);
  const push = (text: string, group: number) => {
    const last = pieces[pieces.length - 1];
    if (last && last.group === group) last.text += text;
    else pieces.push({ text, group });
  };

  let i = 0;
  while (i < chars.length) {
    const c = chars[i];
    if (c === '\\') {
      push(c + (chars[i + 1] ?? ''), current());
      i += 2;
    } else if (c === '[') {
      let j = i + 1;
      if (chars[j] === '^') j++;
      if (chars[j] === ']') j++;
      while (j < chars.length && chars[j] !== ']') j += chars[j] === '\\' ? 2 : 1;
      push(chars.slice(i, j + 1).join(''), current());
      i = j + 1;
    } else if (c === '(') {
      let capturing = true;
      if (chars[i + 1] === '?') {
        const named = chars[i + 2] === 'P' && chars[i + 3] === '<';
        const angle = chars[i + 2] === '<' && chars[i + 3] !== '=' && chars[i + 3] !== '!';
        capturing = named || angle;
      }
      push('(', -1);
      stack.push(capturing ? nextGroup++ : current());
      i += 1;
    } else if (c === ')') {
      stack.pop();
      push(')', -1);
      i += 1;
    } else {
      push(c, current());
      i += 1;
    }
  }
  return { pieces, groupCount: nextGroup - 1 };
}
