"use client";

import React, { Suspense } from "react";

const Home = (): JSX.Element => {
  return <Suspense fallback={<div>Loading...</div>}></Suspense>;
};

export default Home;
