export enum Route {
  home = "/",
  personMatch = "/person_match",
}

export const getRoute = (
  route: Route,
  pathParams?: Record<string, string>,
  queryParams?: Record<string, string>,
): string => {
  let path: string = route;

  if (pathParams) {
    Object.keys(pathParams).forEach((key) => {
      path = path.replace(`{${key}}`, pathParams[key]);
    });
  }
  return path + "?" + new URLSearchParams(queryParams);
};
