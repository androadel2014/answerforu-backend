// src/components/AuthView.jsx
import React, { useMemo, useState } from "react";
import { UserPlus, Mail, Lock, ArrowLeft, ArrowRight } from "lucide-react";
import toast from "react-hot-toast";
import { useNavigate } from "react-router-dom";

export const AuthView = ({ lang = "en" }) => {
  const API_BASE =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_API_BASE ||
    "http://localhost:5000";

  const navigate = useNavigate();

  const normLang = (v) => {
    const s = String(v || "en").toLowerCase();
    if (s.startsWith("ar")) return "ar";
    if (s.startsWith("es")) return "es";
    return "en";
  };
  const LKEY = normLang(lang);
  const dir = LKEY === "ar" ? "rtl" : "ltr";

  const L = useMemo(
    () =>
      ({
        ar: {
          back: "العودة للرئيسية",
          signIn: "تسجيل الدخول",
          createAccount: "إنشاء حساب جديد",
          welcomeBack: "أهلاً بك مجدداً",
          startNow: "ابدأ حسابك الآن",
          name: "الاسم",
          emailOrUser: "الإيميل أو اليوزر",
          email: "الإيميل",
          password: "كلمة المرور",
          fillFields: "املأ البيانات الأول",
          emailValid: "التسجيل لازم يكون بإيميل صحيح",
          processing: "جارٍ التنفيذ...",
          login: "دخول",
          register: "سجل الآن",
          noAccount: "ليس لديك حساب؟ سجل الآن",
          haveAccount: "لديك حساب؟ سجل دخولك",
          wrongCred: "الإيميل/اليوزر أو كلمة السر غلط",
          badReq: "البيانات ناقصة أو الطلب غلط",
          somethingWrong: "حدث خطأ",
          serverNoToken: "السيرفر لم يُرجع Token",
          registered: "تم إنشاء الحساب ✅",
          loggedIn: "تم تسجيل الدخول ✅",
          serverConnFail: "تعذر الاتصال بالسيرفر (شغل الباك أولاً)",
        },
        en: {
          back: "Back to Home",
          signIn: "Sign In",
          createAccount: "Create Account",
          welcomeBack: "Welcome back",
          startNow: "Create your account",
          name: "Name",
          emailOrUser: "Email or Username",
          email: "Email",
          password: "Password",
          fillFields: "Fill the fields",
          emailValid: "Registration requires a valid email",
          processing: "Processing...",
          login: "Login",
          register: "Register",
          noAccount: "Don't have an account? Sign up",
          haveAccount: "Already have an account? Sign in",
          wrongCred: "Wrong credentials",
          badReq: "Bad request — missing/invalid fields",
          somethingWrong: "Something went wrong",
          serverNoToken: "Server did not return a token",
          registered: "Registered ✅",
          loggedIn: "Logged in ✅",
          serverConnFail: "Server connection failed",
        },
        es: {
          back: "Volver al inicio",
          signIn: "Iniciar sesión",
          createAccount: "Crear cuenta",
          welcomeBack: "Bienvenido de nuevo",
          startNow: "Crea tu cuenta",
          name: "Nombre",
          emailOrUser: "Correo o usuario",
          email: "Correo",
          password: "Contraseña",
          fillFields: "Completa los campos",
          emailValid: "El registro requiere un correo válido",
          processing: "Procesando...",
          login: "Entrar",
          register: "Registrarse",
          noAccount: "¿No tienes cuenta? Regístrate",
          haveAccount: "¿Ya tienes cuenta? Inicia sesión",
          wrongCred: "Credenciales incorrectas",
          badReq: "Solicitud inválida — faltan datos",
          somethingWrong: "Algo salió mal",
          serverNoToken: "El servidor no devolvió un token",
          registered: "Registrado ✅",
          loggedIn: "Sesión iniciada ✅",
          serverConnFail: "No se pudo conectar al servidor",
        },
      }[LKEY] || {}),
    [LKEY]
  );

  const [isLogin, setIsLogin] = useState(true);
  const [loading, setLoading] = useState(false);

  const [formData, setFormData] = useState({
    username: "",
    identifier: "",
    password: "",
  });

  const BackIcon = LKEY === "ar" ? ArrowRight : ArrowLeft;

  const switchMode = () => {
    setIsLogin((p) => !p);
    setFormData({ username: "", identifier: "", password: "" });
  };

  const handleChange = (e) => {
    setFormData((p) => ({ ...p, [e.target.name]: e.target.value }));
  };

  const safeJson = async (res) => {
    try {
      return await res.json();
    } catch {
      return {};
    }
  };

  const isEmail = (v) => /.+@.+\..+/.test(String(v || "").trim());

  const normalizeErr = (res, data) => {
    if (res.status === 401) return data?.message || L.wrongCred;
    if (res.status === 400) return data?.message || L.badReq;
    return data?.message || L.somethingWrong;
  };

  const postAuth = async (endpoint, payload) => {
    const res = await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await safeJson(res);
    return { res, data };
  };

  const saveAuth = (data) => {
    const token = data?.token || data?.accessToken || data?.jwt;
    const user = data?.user || data?.data?.user || data?.me;

    if (!token) {
      toast.error(L.serverNoToken);
      return false;
    }

    localStorage.setItem("token", token);
    if (user) localStorage.setItem("user", JSON.stringify(user));

    window.dispatchEvent(new Event("auth_changed"));
    return true;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (loading) return;

    const endpoint = isLogin ? "/api/auth/login" : "/api/auth/register";

    const identifier = String(formData.identifier || "").trim();
    const username = String(formData.username || "").trim();
    const password = String(formData.password || "");

    if (isLogin) {
      if (!identifier || !password) {
        toast.error(L.fillFields);
        return;
      }
    } else {
      if (!username || !identifier || !password) {
        toast.error(L.fillFields);
        return;
      }
      if (!isEmail(identifier)) {
        toast.error(L.emailValid);
        return;
      }
    }

    setLoading(true);

    try {
      if (!isLogin) {
        const payload = { username, email: identifier, password };
        const { res, data } = await postAuth(endpoint, payload);

        if (!res.ok) {
          toast.error(normalizeErr(res, data));
          return;
        }

        if (!saveAuth(data)) return;

        toast.success(L.registered);
        navigate("/", { replace: true });
        return;
      }

      const tryEmailPayload = { email: identifier, password };
      let { res, data } = await postAuth(endpoint, tryEmailPayload);

      if (!res.ok && !isEmail(identifier)) {
        const tryUsernamePayload = { username: identifier, password };
        const retry = await postAuth(endpoint, tryUsernamePayload);
        res = retry.res;
        data = retry.data;
      }

      if (!res.ok) {
        toast.error(normalizeErr(res, data));
        return;
      }

      if (!saveAuth(data)) return;

      toast.success(L.loggedIn);
      navigate("/", { replace: true });
    } catch (err) {
      console.error(err);
      toast.error(L.serverConnFail);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      className="min-h-[80vh] flex items-center justify-center px-3 sm:px-6 animate-[fadeIn_0.4s_ease-out]"
      dir={dir}
    >
      <div className="bg-white w-full max-w-md rounded-3xl shadow-xl border border-slate-100 overflow-hidden mb-12 mt-12">
        <div className="p-5 sm:p-8">
          <button
            type="button"
            onClick={() => navigate("/", { replace: true })}
            className="flex items-center gap-2 text-slate-400 hover:text-blue-600 mb-6 sm:mb-8 transition-colors text-sm font-bold cursor-pointer"
          >
            <BackIcon size={16} />
            {L.back}
          </button>

          <h2 className="text-2xl sm:text-3xl font-black text-slate-900 mb-2">
            {isLogin ? L.signIn : L.createAccount}
          </h2>

          <p className="text-slate-500 mb-6 sm:mb-8 font-medium">
            {isLogin ? L.welcomeBack : L.startNow}
          </p>

          <form className="space-y-4" onSubmit={handleSubmit}>
            {!isLogin && (
              <div>
                <label className="block text-sm font-bold mb-2 pr-1">
                  {L.name}
                </label>
                <div className="relative">
                  <input
                    type="text"
                    name="username"
                    value={formData.username}
                    onChange={handleChange}
                    required={!isLogin}
                    autoComplete="name"
                    className={classNames(
                      "w-full p-4 bg-slate-50 border border-slate-200 rounded-2xl focus:ring-2 focus:ring-blue-500 outline-none",
                      dir === "rtl" ? "pr-12" : "pl-12"
                    )}
                  />
                  <UserPlus
                    className={classNames(
                      "absolute top-4 text-slate-400",
                      dir === "rtl" ? "right-4" : "left-4"
                    )}
                    size={20}
                  />
                </div>
              </div>
            )}

            <div>
              <label className="block text-sm font-bold mb-2 pr-1">
                {isLogin ? L.emailOrUser : L.email}
              </label>
              <div className="relative">
                <input
                  type={isLogin ? "text" : "email"}
                  name="identifier"
                  value={formData.identifier}
                  onChange={handleChange}
                  required
                  autoComplete={isLogin ? "username" : "email"}
                  inputMode={isLogin ? "text" : "email"}
                  className={classNames(
                    "w-full p-4 bg-slate-50 border border-slate-200 rounded-2xl focus:ring-2 focus:ring-blue-500 outline-none",
                    dir === "rtl" ? "pr-12" : "pl-12"
                  )}
                />
                <Mail
                  className={classNames(
                    "absolute top-4 text-slate-400",
                    dir === "rtl" ? "right-4" : "left-4"
                  )}
                  size={20}
                />
              </div>
            </div>

            <div>
              <label className="block text-sm font-bold mb-2 pr-1">
                {L.password}
              </label>
              <div className="relative">
                <input
                  type="password"
                  name="password"
                  value={formData.password}
                  onChange={handleChange}
                  required
                  autoComplete={isLogin ? "current-password" : "new-password"}
                  className={classNames(
                    "w-full p-4 bg-slate-50 border border-slate-200 rounded-2xl focus:ring-2 focus:ring-blue-500 outline-none",
                    dir === "rtl" ? "pr-12" : "pl-12"
                  )}
                />
                <Lock
                  className={classNames(
                    "absolute top-4 text-slate-400",
                    dir === "rtl" ? "right-4" : "left-4"
                  )}
                  size={20}
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={loading}
              className={`w-full p-4 rounded-2xl font-bold text-lg shadow-lg transition-all mt-4 ${
                loading
                  ? "bg-slate-200 text-slate-500 cursor-not-allowed"
                  : "bg-blue-600 text-white hover:bg-blue-700 shadow-blue-100 cursor-pointer"
              }`}
            >
              {loading ? L.processing : isLogin ? L.login : L.register}
            </button>
          </form>
        </div>

        <div className="p-6 bg-slate-50 border-t border-slate-100 text-center">
          <button
            type="button"
            onClick={switchMode}
            className="text-blue-600 font-bold hover:underline cursor-pointer"
          >
            {isLogin ? L.noAccount : L.haveAccount}
          </button>
        </div>
      </div>
    </div>
  );
};

const classNames = (...arr) => arr.filter(Boolean).join(" ");
