import { createStore, StoreApi } from "zustand/vanilla";
import { immer } from "zustand/middleware/immer";
import {
  PersonMatchActions,
  PersonMatchState,
  createPersonMatchSlice,
  defaultInitState as personMatchDefaultInitState,
} from "./person_match_slice";
import { AppStore } from "./types";

export type AppState = PersonMatchState;

export type AppActions = PersonMatchActions;

export const defaultInitState: AppState = {
  ...personMatchDefaultInitState,
};

export const createAppStore = (
  initState: AppState = defaultInitState,
): StoreApi<AppStore> => {
  return createStore<AppStore>()(
    immer((set, get, store) => ({
      ...initState,

      ...createPersonMatchSlice({
        personMatch: initState.personMatch,
      })(set, get, store),
    })),
  );
};
