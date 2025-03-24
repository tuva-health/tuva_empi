export interface DataSource {
  name: string;
}

export interface SearchTerms {
  [key: string]: string | undefined;
  first_name?: string;
  last_name?: string;
  birth_date?: string;
  data_source?: string;
  person_id?: string;
  source_person_id?: string;
}

export interface PersonSummary {
  id: string;
  first_name: string;
  last_name: string;
  data_sources: string[];
}

export interface Person {
  id: string;
  created: Date;
  version: number;
  records: PersonRecord[];
}

export interface PersonRecord {
  id: string;
  created: Date;
  person_id: string;
  person_updated: Date;
  matched_or_reviewed: Date;
  data_source: string;
  source_person_id: string;
  first_name: string;
  last_name: string;
  sex: string;
  race: string;
  birth_date: string;
  death_date: string;
  social_security_number: string;
  address: string;
  city: string;
  state: string;
  zip_code: string;
  county: string;
  phone: string;
}

export interface PredictionResult {
  id: string;
  person_record_l_id: string;
  person_record_r_id: string;
  match_probability: number;
}

export interface PotentialMatchSummary {
  id: string;
  first_name: string;
  last_name: string;
  data_sources: string[];
  max_match_probability: number;
}

export interface PotentialMatch {
  id: string;
  version: number;
  persons: Person[];
  results: PredictionResult[];
}

export interface PersonUpdate {
  id?: string;
  version?: number;
  new_person_record_ids: string[];
}

export interface PersonRecordComment {
  person_recrod_id: string;
  comment: string;
}

export const fetchDataSources = async (): Promise<DataSource[]> => {
  console.info("Fetching data sources");

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const url = apiBaseUrl + "data-sources";

  try {
    const response = await fetch(url);
    const data: { data_sources: DataSource[] } = await response.json();

    return data.data_sources;
  } catch (error) {
    console.error("Error fetching data sources:", error);
    return [];
  }
};

export const fetchPersons = async (
  searchTerms: SearchTerms,
): Promise<PersonSummary[]> => {
  console.info("Searching for Persons with search terms: ", searchTerms);

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const url = apiBaseUrl + "persons";
  const searchParams: Record<string, string> = {};

  if (searchTerms?.first_name) {
    searchParams["first_name"] = searchTerms.first_name;
  }
  if (searchTerms?.last_name) {
    searchParams["last_name"] = searchTerms.last_name;
  }
  if (searchTerms?.birth_date) {
    searchParams["birth_date"] = searchTerms.birth_date;
  }
  if (searchTerms?.data_source) {
    searchParams["data_source"] = searchTerms.data_source;
  }
  if (searchTerms?.person_id) {
    searchParams["person_id"] = searchTerms.person_id;
  }
  if (searchTerms?.source_person_id) {
    searchParams["source_person_id"] = searchTerms.source_person_id;
  }

  const queryString = new URLSearchParams(searchParams).toString();

  try {
    const response = await fetch(url + "?" + queryString);
    const data: { persons: PersonSummary[] } = await response.json();

    return data.persons;
  } catch (error) {
    console.error("Error fetching persons:", error);
    return [];
  }
};

export const fetchPerson = async (id: string): Promise<Person | undefined> => {
  console.info("Fetching Person with id: ", id);

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const url = apiBaseUrl + `persons/${id}`;

  try {
    const response = await fetch(url);
    const data: { person: Person } = await response.json();

    return data.person;
  } catch (error) {
    console.error("Error fetching person:", error);
    return undefined;
  }
};

export const fetchPotentialMatches = async (
  searchTerms?: SearchTerms,
): Promise<PotentialMatchSummary[]> => {
  console.info(
    "Searching for Potential Matches with search terms: ",
    searchTerms,
  );

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const url = apiBaseUrl + "potential-matches";
  const searchParams: Record<string, string> = {};

  if (searchTerms?.first_name) {
    searchParams["first_name"] = searchTerms.first_name;
  }
  if (searchTerms?.last_name) {
    searchParams["last_name"] = searchTerms.last_name;
  }
  if (searchTerms?.birth_date) {
    searchParams["birth_date"] = searchTerms.birth_date;
  }
  if (searchTerms?.data_source) {
    searchParams["data_source"] = searchTerms.data_source;
  }
  if (searchTerms?.person_id) {
    searchParams["person_id"] = searchTerms.person_id;
  }
  if (searchTerms?.source_person_id) {
    searchParams["source_person_id"] = searchTerms.source_person_id;
  }

  const queryString = new URLSearchParams(searchParams).toString();

  try {
    const response = await fetch(url + "?" + queryString);
    const data: { potential_matches: PotentialMatchSummary[] } =
      await response.json();

    return data.potential_matches;
  } catch (error) {
    console.error("Error fetching potential matches:", error);
    return [];
  }
};

export const fetchPotentialMatch = async (
  id: string,
): Promise<PotentialMatch | undefined> => {
  console.info("Fetching Potential Match with id: ", id);

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const url = apiBaseUrl + `potential-matches/${id}`;

  try {
    const response = await fetch(url);
    const data: { potential_match: PotentialMatch } = await response.json();

    return data.potential_match;
  } catch (error) {
    console.error("Error fetching potential match:", error);
    return undefined;
  }
};

export const matchPersonRecords = async (
  potential_match_id: string,
  potential_match_version: number,
  person_updates: PersonUpdate[],
  comments?: PersonRecordComment[],
): Promise<undefined> => {
  console.info("Matching person records");

  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const url = apiBaseUrl + "matches";

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        potential_match_id,
        potential_match_version,
        person_updates,
        comments,
      }),
    });
    if (!response.ok) {
      throw new Error("Error matching person records");
    }
    return;
  } catch (error) {
    console.error("Error matcing person records:", error);
    throw error;
  }
};
