import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { AppHeader } from '../components/layout/AppHeader';
import { Footer } from '../components/layout/Footer';
import { CheckIcon, CopyIcon } from '../components/icons';
import { SearchForm, type SearchDraft } from '../components/search/SearchForm';
import { Button, ButtonLink } from '../components/ui/Button';
import { DEFAULT_PER_PAGE, newSearch, toSearchParams } from '../lib/searchState';
import styles from './HomePage.module.css';

const REGEX_EXAMPLES = [
  { code: 'שלום|היי|מה נשמע', desc: 'אחת מהמילים (או)' },
  { code: '\\bמילה\\b', desc: 'מילה שלמה בדיוק' },
  { code: '^תחילת', desc: 'בתחילת קטע' },
  { code: 'סוף$', desc: 'בסוף קטע' },
  { code: '[א-ת]{3,6}', desc: '3–6 אותיות עבריות ברצף' },
  { code: 'מספר\\s+\\d+', desc: '״מספר״ ואחריו ספרות' },
  { code: '(?i)hello', desc: 'בלי הבחנה בין אותיות גדולות וקטנות' },
  { code: 'א.ב', desc: 'א, תו כלשהו, ב' },
  { code: 'מילה.*אחרת', desc: '״מילה״ ואחר כך ״אחרת״' },
  { code: '(שלום|היי)\\s+(עולם|חברים)', desc: 'שילוב קבוצות: ברכה + נמען' },
  { code: '\\d{2,4}', desc: '2–4 ספרות' },
  { code: '[א-ת]+@[a-z]+\\.[a-z]{2,}', desc: 'כתובת מייל' },
];

const CITATION =
  'Yanir Marmor, Yair Lifshitz, Yoad Snapir, and Kinneret Misgav. 2025. Building an Accurate Open-Source Hebrew ASR System through Crowdsourcing. Proceedings of Interspeech 2025';

export function HomePage() {
  const navigate = useNavigate();
  const [draft, setDraft] = useState<SearchDraft>({ q: '', mode: 'exact', perPage: DEFAULT_PER_PAGE, shuffle: true });
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    document.title = 'ivrit.ai Explore';
  }, []);

  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1500);
    return () => clearTimeout(t);
  }, [copied]);

  const submit = (d: SearchDraft) => navigate(`/search?${toSearchParams(newSearch(d.q, d.mode, d))}`);

  const applyExample = (code: string) => {
    setDraft((d) => ({ ...d, q: code, mode: 'regex' }));
    document.getElementById('home-search')?.scrollIntoView({ behavior: 'smooth', block: 'center' });
  };

  return (
    <div className={styles.page}>
      <AppHeader />
      <main className={styles.main}>
        <div className={styles.top}>
          <section id="about" className={styles.about} aria-labelledby="about-h">
            <h1 id="about-h" className={styles.aboutTitle}>
              אודות ivrit.ai
            </h1>
            <p>
              ivrit.ai הוא פרויקט ללא מטרות רווח. מטרתנו היא להקל על תמיכה איכותית בעברית בכלי בינה מלאכותית, ובעיקר ביכולות תמלול,
              על ידי הנגשת מאגרי מידע איכותיים בעברית ברישיון נוח לשימוש.
            </p>
            <p>ממשק זה מאפשר חיפוש ועיון במאגרי המידע שנאספו במסגרת המיזם ותומללו בעזרת מכונה.</p>
            <div className={styles.links}>
              <ButtonLink href="https://www.ivrit.ai/en/the-license/" target="_blank" rel="noopener noreferrer">
                רישיון ivrit.ai
              </ButtonLink>
              <ButtonLink href="https://huggingface.co/ivrit-ai" target="_blank" rel="noopener noreferrer">
                מאגר Huggingface
              </ButtonLink>
              <ButtonLink href="mailto:info@ivrit.ai" className="mono">
                info@ivrit.ai
              </ButtonLink>
            </div>
          </section>
          <div id="home-search" className={styles.search}>
            <SearchForm variant="hero" value={draft} onChange={setDraft} onSubmit={submit} />
          </div>
        </div>

        <section className={styles.section} aria-labelledby="rx-h">
          <div className={styles.sectionHead}>
            <div>
              <h2 id="rx-h" className={styles.sectionTitle}>
                דוגמאות Regex
              </h2>
              <p className={styles.sectionSub}>
                תחביר Python מלא (ספריית <code>regex</code>). לחיצה על שורה מעבירה אותה לחיפוש.
              </p>
            </div>
            <ButtonLink href="https://regex101.com/?flavor=python" target="_blank" rel="noopener noreferrer">
              בדיקה ב-regex101
            </ButtonLink>
          </div>
          <div className={styles.examples}>
            {REGEX_EXAMPLES.map((ex) => (
              <button key={ex.code} type="button" className={styles.example} onClick={() => applyExample(ex.code)}>
                <span className={styles.exampleDesc}>{ex.desc}</span>
                <code dir="ltr" className={styles.exampleCode}>
                  {ex.code}
                </code>
              </button>
            ))}
          </div>
          <p className={styles.note}>
            תווים מיוחדים שדורשים <code dir="ltr">\</code> לפניהם: <code dir="ltr">. * + ? [ ] ( ) {'{ }'} | \ ^ $</code>
          </p>
        </section>

        <section className={`${styles.section} ${styles.cite}`} aria-labelledby="cite-h">
          <div className={styles.citeText}>
            <h2 id="cite-h" className={styles.sectionTitle}>
              ציטוט במאמרים
            </h2>
            <p className={styles.sectionSub}>לטובת פרסום מדעי, נשמח אם תצטטו את השימוש במאגר או במערכת כך:</p>
          </div>
          <div className={styles.citeBox}>
            <p dir="ltr" className={styles.citation}>
              {CITATION}
            </p>
            <Button icon={copied ? <CheckIcon size={16} /> : <CopyIcon size={16} />} onClick={() => navigator.clipboard?.writeText(CITATION).then(() => setCopied(true))}>
              {copied ? 'הועתק' : 'העתקה'}
            </Button>
          </div>
        </section>
      </main>
      <Footer />
    </div>
  );
}
