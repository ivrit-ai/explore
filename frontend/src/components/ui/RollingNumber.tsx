import styles from './RollingNumber.module.css';

const DIGITS = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

/** Each digit is a vertical strip that rolls to its new value (number tick). */
export function RollingNumber({ value }: { value: number }) {
  const text = Math.max(0, Math.round(value)).toLocaleString('en-US');
  return (
    <span className={styles.number}>
      <span className="sr-only">{text}</span>
      <span aria-hidden="true" className={styles.digits}>
        {Array.from(text).map((ch, i) =>
          /\d/.test(ch) ? (
            <span key={text.length - i} className={styles.window}>
              <span className={styles.strip} style={{ transform: `translateY(${-Number(ch)}em)` }}>
                {DIGITS.map((d) => (
                  <span key={d}>{d}</span>
                ))}
              </span>
            </span>
          ) : (
            <span key={`sep-${text.length - i}`}>{ch}</span>
          ),
        )}
      </span>
    </span>
  );
}
