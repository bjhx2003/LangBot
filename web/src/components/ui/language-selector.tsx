'use client';

import { useState, useEffect } from 'react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Globe } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';

interface LanguageSelectorProps {
  className?: string;
  triggerClassName?: string;
  onOpenChange?: (open: boolean) => void;
}

export function LanguageSelector({
  triggerClassName,
  onOpenChange,
}: LanguageSelectorProps) {
  const { t } = useTranslation();
  const [currentLanguage, setCurrentLanguage] = useState<string>(i18n.language);

  useEffect(() => {
    initializeLanguage();
  }, []);

  const initializeLanguage = () => {
    if (i18n.language === 'zh-CN' || i18n.language === 'zh-Hans') {
      setCurrentLanguage('zh-Hans');
      localStorage.setItem('langbot_language', 'zh-Hans');
    } else if (i18n.language === 'zh-TW' || i18n.language === 'zh-Hant') {
      setCurrentLanguage('zh-Hant');
      localStorage.setItem('langbot_language', 'zh-Hant');
    } else if (i18n.language === 'ja' || i18n.language === 'ja-JP') {
      setCurrentLanguage('ja-JP');
      localStorage.setItem('langbot_language', 'ja-JP');
    } else {
      setCurrentLanguage('en-US');
      localStorage.setItem('langbot_language', 'en-US');
    }

    const savedLanguage = localStorage.getItem('langbot_language');
    if (savedLanguage) {
      i18n.changeLanguage(savedLanguage);
      setCurrentLanguage(savedLanguage);
    } else {
      const browserLanguage = navigator.language;
      if (browserLanguage) {
        let detectedLanguage = 'zh-Hans';
        if (browserLanguage === 'zh-CN') {
          detectedLanguage = 'zh-Hans';
        } else if (browserLanguage === 'zh-TW') {
          detectedLanguage = 'zh-Hant';
        } else if (browserLanguage === 'ja' || browserLanguage === 'ja-JP') {
          detectedLanguage = 'ja-JP';
        } else {
          detectedLanguage = 'en-US';
        }
        i18n.changeLanguage(detectedLanguage);
        setCurrentLanguage(detectedLanguage);
        localStorage.setItem('langbot_language', detectedLanguage);
      }
    }
  };

  const handleLanguageChange = (value: string) => {
    i18n.changeLanguage(value);
    setCurrentLanguage(value);
    localStorage.setItem('langbot_language', value);

    // 刷新页面以应用新的语言设置
    window.location.reload();
  };

  return (
    <Select
      value={currentLanguage}
      onValueChange={handleLanguageChange}
      onOpenChange={onOpenChange}
    >
      <SelectTrigger className={triggerClassName || 'w-[140px]'}>
        <Globe className="h-4 w-4 mr-2" />
        <SelectValue placeholder={t('common.language')} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="zh-Hans">简体中文</SelectItem>
        <SelectItem value="zh-Hant">繁體中文</SelectItem>
        <SelectItem value="en-US">English</SelectItem>
        <SelectItem value="ja-JP">日本語</SelectItem>
      </SelectContent>
    </Select>
  );
}
