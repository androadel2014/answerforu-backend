export const getToken = () => localStorage.getItem("token");
export const isLoggedIn = () => !!localStorage.getItem("token");
export const logout = () => {
  localStorage.removeItem("token");
  localStorage.removeItem("user");
  window.location.href = "/login";
};
