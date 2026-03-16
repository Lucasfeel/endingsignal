import { useState } from "react";

import type { NavTab } from "../config";

export function usePublicUiState() {
  const [activeTab, setActiveTab] = useState<NavTab>("home");
  const [selectedSources, setSelectedSources] = useState<Record<NavTab, string[]>>({
    home: [],
    webtoon: [],
    novel: [],
    ott: [],
    my: [],
  });
  const [webtoonFilter, setWebtoonFilter] = useState("all");
  const [novelFilter, setNovelFilter] = useState("all");
  const [ottFilter, setOttFilter] = useState("all");
  const [myViewMode, setMyViewMode] = useState<"completion" | "completed">("completion");
  const [searchInput, setSearchInput] = useState("");
  const [authMode, setAuthMode] = useState<"login" | "register">("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authPasswordConfirm, setAuthPasswordConfirm] = useState("");
  const [authError, setAuthError] = useState("");

  function resetAuthForm() {
    setAuthEmail("");
    setAuthPassword("");
    setAuthPasswordConfirm("");
  }

  return {
    activeTab,
    authEmail,
    authError,
    authMode,
    authPassword,
    authPasswordConfirm,
    myViewMode,
    novelFilter,
    ottFilter,
    resetAuthForm,
    searchInput,
    selectedSources,
    setActiveTab,
    setAuthEmail,
    setAuthError,
    setAuthMode,
    setAuthPassword,
    setAuthPasswordConfirm,
    setMyViewMode,
    setNovelFilter,
    setOttFilter,
    setSearchInput,
    setSelectedSources,
    setWebtoonFilter,
    webtoonFilter,
  };
}
