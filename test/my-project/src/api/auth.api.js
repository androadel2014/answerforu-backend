import { apiRequest } from "./http";

export const authApi = {
  login: (data) =>
    apiRequest("/api/auth/login", {
      method: "POST",
      body: data,
    }),

  register: (data) =>
    apiRequest("/api/auth/register", {
      method: "POST",
      body: data,
    }),
};
