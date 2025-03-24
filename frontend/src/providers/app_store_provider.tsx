"use client";

import React, {
  type ReactNode,
  createContext,
  useRef,
  useContext,
} from "react";
import { useStore } from "zustand";

import { createAppStore } from "@/lib/stores/app_store";
import { type AppStore } from "@/lib/stores/types";

export type AppStoreApi = ReturnType<typeof createAppStore>;

export const AppStoreContext = createContext<AppStoreApi | undefined>(
  undefined,
);

export interface AppStoreProviderProps {
  children: ReactNode;
}

export const AppStoreProvider = ({
  children,
}: AppStoreProviderProps): React.ReactNode => {
  const storeRef = useRef<AppStoreApi | null>(null);

  if (!storeRef.current) {
    storeRef.current = createAppStore();
  }

  return (
    <AppStoreContext.Provider value={storeRef.current}>
      {children}
    </AppStoreContext.Provider>
  );
};

export const useAppStore = <T,>(selector: (store: AppStore) => T): T => {
  const appStoreContext = useContext(AppStoreContext);

  if (!appStoreContext) {
    throw new Error(`useAppStore must be used within AppStoreProvider`);
  }

  return useStore(appStoreContext, selector);
};
