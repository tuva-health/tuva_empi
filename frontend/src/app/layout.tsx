import React, { FC } from "react";
import type { Metadata } from "next";
import "@/css/app.css";
import { AppStoreProvider } from "@/providers/app_store_provider";
import { Albert_Sans } from "next/font/google";

const albertSansFont = Albert_Sans({
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Tuva Health Tuva EMPI",
  description: "Tuva Health Tuva EMPI",
};

const RootLayout: FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <html lang="en" className={albertSansFont.className}>
      <body>
        <AppStoreProvider>{children}</AppStoreProvider>
      </body>
    </html>
  );
};

export default RootLayout;
