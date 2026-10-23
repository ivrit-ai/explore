import { useEffect } from 'react';
import { AppHeader } from '../components/layout/AppHeader';
import { Footer } from '../components/layout/Footer';
import styles from './PrivacyPage.module.css';

export function PrivacyPage() {
  useEffect(() => {
    document.title = 'מדיניות פרטיות — ivrit.ai Explore';
  }, []);

  return (
    <div className={styles.page}>
      {/* Readable without logging in, so no account menu */}
      <AppHeader signedIn={false} />
      <main className={styles.main}>
        <h1 className={styles.title}>מדיניות פרטיות</h1>

        <section>
          <h2>איסוף נתונים</h2>
          <p>אתר זה אוסף נתוני שימוש כדי לשפר את השירות שלנו ולהבין כיצד משתמשים באתר. כולל:</p>
          <ul>
            <li>שאילתות חיפוש</li>
            <li>צפיות והאזנות בדפים וניווט באתר</li>
            <li>אינטראקציות של המשתמש עם הממשק</li>
            <li>מדדי ביצועים ומידע על שגיאות</li>
          </ul>
        </section>

        <section>
          <h2>כיצד אנו משתמשים בנתונים שלך</h2>
          <p>הנתונים שאנו אוספים משמשים ל:</p>
          <ul>
            <li>שיפור פונקציונליות החיפוש והביצועים</li>
            <li>שיפור חוויית המשתמש</li>
            <li>זיהוי ותיקון שגיאות</li>
            <li>הבנת דפוסי שימוש להכוונת הפיתוח</li>
          </ul>
        </section>

        <section>
          <h2>שיתוף נתונים</h2>
          <p>איננו מוכרים או משתפים את הנתונים האישיים שלך עם צדדים שלישיים. נתוני האנליטיקה מעובדים באמצעות PostHog, ספק האנליטיקה שלנו.</p>
        </section>

        <section>
          <h2>הזכויות שלך</h2>
          <p>יש לך את הזכות ל:</p>
          <ul>
            <li>גישה לנתונים שאנו אוספים עליך</li>
            <li>בקשת מחיקה של הנתונים שלך</li>
          </ul>
          <p>
            כדי לממש זכויות אלה, ניתן ליצור קשר בכתובת <a href="mailto:info@ivrit.ai">info@ivrit.ai</a>.
          </p>
        </section>

        <section>
          <h2>שינויים במדיניות זו</h2>
          <p>אנו עשויים לעדכן את מדיניות הפרטיות הזו מעת לעת. אנו נודיע למשתמשים על כל שינוי משמעותי על ידי פרסום הודעה באתר שלנו.</p>
        </section>

        <section>
          <h2>צור קשר</h2>
          <p>
            אם יש לך שאלות כלשהן לגבי מדיניות פרטיות זו, ניתן ליצור קשר בכתובת <a href="mailto:info@ivrit.ai">info@ivrit.ai</a>.
          </p>
        </section>
      </main>
      <Footer />
    </div>
  );
}
