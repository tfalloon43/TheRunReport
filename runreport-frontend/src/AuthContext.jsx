import { createContext, useContext } from "react";

const AuthContext = createContext({ session: null, loading: true });

export function useAuth() {
  return useContext(AuthContext);
}

export default AuthContext;
