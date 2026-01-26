// src/components/Data.jsx
import {
  Home,
  Sparkles,
  Map,
  Shield,
  Briefcase,
  Building,
  GraduationCap,
  CreditCard,
  Bus,
  HeartHandshake,
  Rss,
} from "lucide-react";

export const DATA = {
  nav: [
    {
      id: "home",
      icon: Home,
      label: { ar: "الرئيسية", en: "Home", es: "Inicio" },
      color: "bg-blue-500",
    },

    {
      id: "feed",
      icon: Rss,
      label: { ar: "صفحه التواصل الاجتماعي", en: "Feed", es: "Social" },
      color: "bg-pink-500",
    },

    {
      id: "cv_builder",
      icon: Sparkles,
      label: { ar: "مُصمم الـ CV", en: "CV Builder", es: "Creador de CV" },
      color: "bg-purple-600",
    },

    {
      id: "start",
      icon: Map,
      label: { ar: "خارطة الطريق", en: "Roadmap", es: "Ruta" },
      color: "bg-indigo-500",
    },
    {
      id: "essentials",
      icon: Shield,
      label: { ar: "الأساسيات", en: "Essentials", es: "Esenciales" },
      color: "bg-emerald-500",
    },
    {
      id: "jobs",
      icon: Briefcase,
      label: { ar: "العمل", en: "Jobs", es: "Trabajo" },
      color: "bg-sky-500",
    },
    {
      id: "housing",
      icon: Building,
      label: { ar: "السكن", en: "Housing", es: "Vivienda" },
      color: "bg-rose-500",
    },

    {
      id: "education",
      icon: GraduationCap,
      label: { ar: "التعليم", en: "Education", es: "Educación" },
      color: "bg-violet-500",
    },
    {
      id: "finance",
      icon: CreditCard,
      label: { ar: "المال", en: "Finance", es: "Finanzas" },
      color: "bg-amber-500",
    },
    {
      id: "transportation",
      icon: Bus,
      label: { ar: "النقل", en: "Transport", es: "Transporte" },
      color: "bg-orange-500",
    },

    {
      id: "community",
      icon: HeartHandshake,
      label: { ar: "المجتمع", en: "Community", es: "Comunidad" },
      color: "bg-teal-500",
    },
  ],

  hero: {
    title: {
      ar: "بدايتك الصحيحة في أمريكا",
      en: "Your Right Start in the USA",
      es: "Tu mejor comienzo en EE. UU.",
    },
    subtitle: {
      ar: "دليل شامل ومبسط يساعدك على فهم النظام، الاستقرار، والنجاح من اليوم الأول.",
      en: "A comprehensive and simplified guide helping you understand the system, settle, and succeed from day one.",
      es: "Una guía completa y sencilla para entender el sistema, establecerte y tener éxito desde el primer día.",
    },
  },

  sections_content: {
    start: {
      title: {
        ar: "خارطة الطريق (أول 90 يوم)",
        en: "The Roadmap (First 90 Days)",
        es: "La Ruta (Primeros 90 días)",
      },
      desc: {
        ar: "خطوات مرتبة زمنياً لضمان عدم نسيان أي إجراء مهم.",
        en: "Chronological steps to ensure nothing important is missed.",
        es: "Pasos cronológicos para no olvidar nada importante.",
      },
      steps: [
        {
          title: {
            ar: "أول 24 ساعة",
            en: "First 24 Hours",
            es: "Primeras 24 horas",
          },
          items: {
            ar: [
              "ختم الجواز في المطار (CBP)",
              "شراء شريحة هاتف (SIM)",
              "الوصول للسكن المؤقت",
            ],
            en: [
              "Passport stamp at the airport (CBP)",
              "Get a SIM card",
              "Arrive to temporary housing",
            ],
            es: [
              "Sello del pasaporte en el aeropuerto (CBP)",
              "Comprar una SIM",
              "Llegar al alojamiento temporal",
            ],
          },
        },
        {
          title: {
            ar: "الأسبوع الأول",
            en: "First Week",
            es: "Primera semana",
          },
          items: {
            ar: [
              "فتح حساب بنكي",
              "التقديم على SSN",
              "استكشاف المنطقة والمواصلات",
            ],
            en: [
              "Open a bank account",
              "Apply for SSN",
              "Explore the area & transportation",
            ],
            es: [
              "Abrir una cuenta bancaria",
              "Solicitar el SSN",
              "Explorar la zona y el transporte",
            ],
          },
        },
        {
          title: { ar: "الشهر الأول", en: "First Month", es: "Primer mes" },
          items: {
            ar: [
              "البحث عن سكن دائم",
              "إصدار هوية الولاية (State ID)",
              "تجهيز السيرة الذاتية",
            ],
            en: [
              "Find long-term housing",
              "Get a State ID",
              "Prepare your resume",
            ],
            es: [
              "Buscar vivienda permanente",
              "Obtener una identificación estatal",
              "Preparar tu currículum",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "احتفظ بنسخ من جميع أوراقك",
          "لا تحمل مبالغ نقدية كبيرة",
          "حمل تطبيقات الخرائط والترجمة",
        ],
        en: [
          "Keep copies of all documents",
          "Avoid carrying large amounts of cash",
          "Install maps & translation apps",
        ],
        es: [
          "Guarda copias de tus documentos",
          "Evita llevar mucho efectivo",
          "Instala apps de mapas y traducción",
        ],
      },
    },

    essentials: {
      title: {
        ar: "الأساسيات القانونية",
        en: "Legal Essentials",
        es: "Esenciales legales",
      },
      desc: {
        ar: "الوثائق والهويات التي تحتاجها للعيش والعمل بشكل قانوني.",
        en: "Documents and IDs you need to live and work legally.",
        es: "Documentos e identificaciones para vivir y trabajar legalmente.",
      },
      steps: [
        {
          title: {
            ar: "رقم الضمان الاجتماعي (SSN)",
            en: "Social Security Number (SSN)",
            es: "Número de Seguro Social (SSN)",
          },
          items: {
            ar: [
              "هو مفتاح العمل والائتمان",
              "التقديم مجاني في مكتب SSA",
              "يصل بالبريد خلال أسبوعين",
            ],
            en: [
              "Key for work and credit",
              "Free to apply at SSA office",
              "Arrives by mail in about 2 weeks",
            ],
            es: [
              "Clave para trabajo y crédito",
              "Solicitud gratis en la oficina SSA",
              "Llega por correo en ~2 semanas",
            ],
          },
        },
        {
          title: {
            ar: "هوية الولاية (State ID)",
            en: "State ID",
            es: "ID estatal",
          },
          items: {
            ar: [
              "بديل للجواز في المعاملات اليومية",
              "تستخرج من الـ DMV",
              "تحتاج إثبات سكن (فواتير/عقد)",
            ],
            en: [
              "Daily alternative to passport",
              "Issued by the DMV",
              "Needs proof of address (bills/lease)",
            ],
            es: [
              "Alternativa al pasaporte en el día a día",
              "Se obtiene en el DMV",
              "Necesita prueba de domicilio (facturas/contrato)",
            ],
          },
        },
        {
          title: {
            ar: "نظام الضرائب (IRS)",
            en: "Taxes (IRS)",
            es: "Impuestos (IRS)",
          },
          items: {
            ar: [
              "يجب تقديم إقرار ضريبي سنوياً",
              "التهرب الضريبي جريمة فيدرالية",
              "احتفظ بفواتيرك المهمة",
            ],
            en: [
              "File taxes yearly",
              "Tax evasion is a federal crime",
              "Keep important receipts",
            ],
            es: [
              "Declarar impuestos cada año",
              "La evasión fiscal es un delito federal",
              "Guarda recibos importantes",
            ],
          },
        },
      ],
      tips: {
        ar: ["لا تشارك رقم SSN إلا مع الجهات الرسمية والبنوك فقط"],
        en: ["Only share SSN with official entities and banks"],
        es: ["Comparte tu SSN solo con entidades oficiales y bancos"],
      },
    },

    jobs: {
      title: {
        ar: "العمل والوظائف",
        en: "Work & Jobs",
        es: "Trabajo y empleo",
      },
      desc: {
        ar: "كيف تجد عملاً، تفهم حقوقك، وتبني مستقبلك المهني.",
        en: "How to find work, understand your rights, and build your career.",
        es: "Cómo encontrar trabajo, entender tus derechos y construir tu carrera.",
      },
      steps: [
        {
          title: { ar: "تجهيز نفسك", en: "Get ready", es: "Prepárate" },
          items: {
            ar: [
              "سيرة ذاتية (Resume) بنمط أمريكي",
              "حساب LinkedIn محدث",
              "معرفة وضعك القانوني للعمل",
            ],
            en: [
              "US-style resume",
              "Updated LinkedIn profile",
              "Know your legal work status",
            ],
            es: [
              "Currículum estilo USA",
              "Perfil de LinkedIn actualizado",
              "Conoce tu estatus legal de trabajo",
            ],
          },
        },
        {
          title: { ar: "البحث عن عمل", en: "Find jobs", es: "Buscar empleo" },
          items: {
            ar: [
              "مواقع Indeed و Monster",
              "مجموعات التوظيف المحلية",
              "العلاقات الشخصية (Networking)",
            ],
            en: ["Indeed & Monster", "Local hiring groups", "Networking"],
            es: ["Indeed y Monster", "Grupos locales de empleo", "Networking"],
          },
        },
        {
          title: {
            ar: "أنواع العقود",
            en: "Contract types",
            es: "Tipos de contrato",
          },
          items: {
            ar: [
              "W2 (موظف دائم بخصم ضرائب)",
              "1099 (عمل حر/مستقل)",
              "Cash (غير رسمي وغالباً غير قانوني)",
            ],
            en: [
              "W2 (employee)",
              "1099 (independent contractor)",
              "Cash (often risky/illegal)",
            ],
            es: [
              "W2 (empleado)",
              "1099 (autónomo/contratista)",
              "Efectivo (a menudo riesgoso/ilegal)",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "لا تدفع مالاً أبداً للحصول على وظيفة",
          "اعرف الحد الأدنى للأجور في ولايتك",
        ],
        en: ["Never pay money to get a job", "Know your state minimum wage"],
        es: [
          "Nunca pagues para conseguir trabajo",
          "Conoce el salario mínimo de tu estado",
        ],
      },
    },

    housing: {
      title: {
        ar: "السكن والإيجار",
        en: "Housing & Rent",
        es: "Vivienda y alquiler",
      },
      desc: {
        ar: "دليل استئجار شقة، فهم العقود، وتجنب الاحتيال.",
        en: "Guide to renting, understanding leases, and avoiding scams.",
        es: "Guía para alquilar, entender contratos y evitar estafas.",
      },
      steps: [
        {
          title: {
            ar: "المصطلحات المهمة",
            en: "Key terms",
            es: "Términos clave",
          },
          items: {
            ar: [
              "Lease (عقد الإيجار)",
              "Deposit (مبلغ التأمين)",
              "Utilities (المرافق: كهرباء/ماء)",
            ],
            en: ["Lease", "Deposit", "Utilities (electric/water)"],
            es: ["Contrato (Lease)", "Depósito", "Servicios (luz/agua)"],
          },
        },
        {
          title: {
            ar: "المستندات المطلوبة",
            en: "Required documents",
            es: "Documentos requeridos",
          },
          items: {
            ar: [
              "إثبات دخل (راتب/حساب بنكي)",
              "تحقق ائتماني (Credit Check)",
              "هوية سارية",
            ],
            en: ["Proof of income", "Credit check", "Valid ID"],
            es: ["Prueba de ingresos", "Revisión de crédito", "ID vigente"],
          },
        },
        {
          title: {
            ar: "تحذيرات الاحتيال",
            en: "Scam warnings",
            es: "Alertas de estafa",
          },
          items: {
            ar: [
              "لا ترسل مالاً قبل رؤية الشقة",
              "تأكد من هوية المالك",
              "اقرأ العقد جيداً قبل التوقيع",
            ],
            en: [
              "Never send money before seeing the place",
              "Verify the landlord",
              "Read the lease carefully",
            ],
            es: [
              "No envíes dinero antes de ver el lugar",
              "Verifica al propietario",
              "Lee el contrato antes de firmar",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "صور الشقة فيديو قبل السكن لتوثيق حالتها",
          "تأكد من شمولية الفواتير في الإيجار",
        ],
        en: [
          "Record a video before moving in",
          "Confirm what utilities are included",
        ],
        es: [
          "Graba un video antes de mudarte",
          "Confirma qué servicios están incluidos",
        ],
      },
    },

    education: {
      title: {
        ar: "التعليم واللغة",
        en: "Education & Language",
        es: "Educación e idioma",
      },
      desc: {
        ar: "تطوير اللغة الإنجليزية، معادلة الشهادات، والمدارس.",
        en: "Improving English, degree equivalency, and schools.",
        es: "Mejorar el inglés, equivalencias y المدارس.",
      },
      steps: [
        {
          title: {
            ar: "تعلم الإنجليزية",
            en: "Learn English",
            es: "Aprende inglés",
          },
          items: {
            ar: [
              "مكتبات عامة (فصول مجانية)",
              "كليات المجتمع (Community College)",
              "تطبيقات (Duolingo/Cambly)",
            ],
            en: [
              "Public libraries (free classes)",
              "Community College",
              "Apps (Duolingo/Cambly)",
            ],
            es: [
              "Bibliotecas (clases gratis)",
              "Community College",
              "Apps (Duolingo/Cambly)",
            ],
          },
        },
        {
          title: { ar: "التعليم الجامعي", en: "College", es: "Universidad" },
          items: {
            ar: [
              "معادلة الشهادة (WES)",
              "المنح الدراسية (FAFSA)",
              "القروض الطلابية",
            ],
            en: [
              "Degree evaluation (WES)",
              "Financial aid (FAFSA)",
              "Student loans",
            ],
            es: [
              "Equivalencia (WES)",
              "Ayuda financiera (FAFSA)",
              "Préstamos estudiantiles",
            ],
          },
        },
        {
          title: {
            ar: "مدارس الأطفال",
            en: "Kids schools",
            es: "Escuelas para niños",
          },
          items: {
            ar: [
              "التسجيل حسب المنطقة السكنية",
              "توفير كارت التطعيمات",
              "باصات المدارس المجانية",
            ],
            en: [
              "Zoned enrollment",
              "Provide vaccination records",
              "Free school buses",
            ],
            es: [
              "Inscripción por zona",
              "Registro de vacunas",
              "Autobuses escolares gratis",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "اللغة هي مفتاح الفرص الأكبر",
          "استفد من المكتبات العامة فهي كنز مجاني",
        ],
        en: [
          "Language unlocks bigger opportunities",
          "Public libraries are a free treasure",
        ],
        es: [
          "El idioma abre más oportunidades",
          "Las bibliotecas públicas son un tesoro gratis",
        ],
      },
    },

    finance: {
      title: {
        ar: "المال والائتمان",
        en: "Finance & Credit",
        es: "Finanzas y crédito",
      },
      desc: {
        ar: "كيف يعمل النظام البنكي وبناء السجل الائتماني (Credit Score).",
        en: "Banking system and building Credit Score.",
        es: "Sistema bancario y cómo construir tu puntaje de crédito.",
      },
      steps: [
        {
          title: { ar: "الحساب البنكي", en: "Banking", es: "Banca" },
          items: {
            ar: [
              "Checking (جاري للمصروفات)",
              "Savings (توفير)",
              "تطبيقات التحويل (Zelle/Venmo)",
            ],
            en: ["Checking", "Savings", "Transfers (Zelle/Venmo)"],
            es: ["Cuenta corriente", "Ahorros", "Transferencias (Zelle/Venmo)"],
          },
        },
        {
          title: {
            ar: "بناء الكريدت",
            en: "Build credit",
            es: "Construir crédito",
          },
          items: {
            ar: [
              "استخرج Secured Credit Card",
              "استخدم 30% فقط من الحد",
              "ادفع الفاتورة كاملة وفي وقتها",
            ],
            en: [
              "Get a secured credit card",
              "Use under 30% of limit",
              "Pay in full & on time",
            ],
            es: [
              "Consigue una tarjeta asegurada",
              "Usa menos del 30%",
              "Paga completo y a tiempo",
            ],
          },
        },
        {
          title: { ar: "الميزانية", en: "Budgeting", es: "Presupuesto" },
          items: {
            ar: ["قاعدة 50/30/20", "صندوق الطوارئ", "تجنب الديون الاستهلاكية"],
            en: ["50/30/20 rule", "Emergency fund", "Avoid consumer debt"],
            es: [
              "Regla 50/30/20",
              "Fondo de emergencia",
              "Evita deudas de consumo",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "الكريدت سكور يؤثر على إيجارك، سيارتك، وتأمينك",
          "لا تفتح حسابات بنكية كثيرة بلا داع",
        ],
        en: [
          "Credit score affects rent, cars, and insurance",
          "Don’t open too many bank accounts",
        ],
        es: [
          "El puntaje afecta alquiler, autos y seguro",
          "No abras muchas cuentas sin necesidad",
        ],
      },
    },

    transportation: {
      title: {
        ar: "المواصلات والقيادة",
        en: "Transport & Driving",
        es: "Transporte y conducción",
      },
      desc: {
        ar: "شراء السيارات، استخراج الرخصة، والمواصلات العامة.",
        en: "Buying cars, getting license, and public transit.",
        es: "Comprar autos, obtener licencia y transporte público.",
      },
      steps: [
        {
          title: {
            ar: "رخصة القيادة",
            en: "Driver’s license",
            es: "Licencia de conducir",
          },
          items: {
            ar: [
              "اختبار نظري (كمبيوتر)",
              "اختبار عملي (قيادة)",
              "تختلف القوانين قليلاً بين الولايات",
            ],
            en: ["Written test", "Road test", "Rules vary by state"],
            es: [
              "Examen teórico",
              "Examen práctico",
              "Las reglas varían por estado",
            ],
          },
        },
        {
          title: { ar: "شراء سيارة", en: "Buy a car", es: "Comprar un auto" },
          items: {
            ar: [
              "شراء من مالك (أرخص)",
              "شراء من معرض (أضمن)",
              "الفحص الميكانيكي ضروري",
            ],
            en: [
              "Private seller (cheaper)",
              "Dealer (safer)",
              "Pre-purchase inspection is important",
            ],
            es: [
              "Dueño (más barato)",
              "Concesionario (más seguro)",
              "Inspección mecánica es clave",
            ],
          },
        },
        {
          title: { ar: "التأمين", en: "Insurance", es: "Seguro" },
          items: {
            ar: [
              "إجباري في معظم الولايات",
              "يتأثر بسجلك وعمرك",
              "قارن الأسعار (Geico/Progressive)",
            ],
            en: [
              "Required in most states",
              "Depends on record & age",
              "Compare (Geico/Progressive)",
            ],
            es: [
              "Obligatorio en muchos estados",
              "Depende de historial y edad",
              "Compara (Geico/Progressive)",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "القيادة بدون تأمين مخاطرة هائلة",
          "احذر من السيارات الغارقة (Salvage Title)",
        ],
        en: ["Driving uninsured is a huge risk", "Beware salvage titles"],
        es: [
          "Conducir sin seguro es muy riesgoso",
          "Cuidado con títulos salvage",
        ],
      },
    },

    community: {
      title: {
        ar: "المجتمع والحياة",
        en: "Community & Life",
        es: "Comunidad y vida",
      },
      desc: {
        ar: "التواصل مع الجالية، الأنشطة، والاندماج في المجتمع.",
        en: "Connecting with community, activities, and integration.",
        es: "Conectar con la comunidad, actividades e integración.",
      },
      steps: [
        {
          title: { ar: "التواصل", en: "Connect", es: "Conectar" },
          items: {
            ar: [
              "مجموعات الفيسبوك المحلية",
              "المساجد والكنائس والمراكز",
              "التطوع في المنظمات",
            ],
            en: [
              "Local Facebook groups",
              "Mosques/churches/centers",
              "Volunteer organizations",
            ],
            es: [
              "Grupos locales de Facebook",
              "Mezquitas/iglesias/centros",
              "Voluntariado",
            ],
          },
        },
        {
          title: { ar: "التسوق", en: "Shopping", es: "Compras" },
          items: {
            ar: ["Walmart/Target (عام)", "Costco (جملة)", "متاجر عربية (حلال)"],
            en: ["Walmart/Target", "Costco (bulk)", "Arabic/Halal stores"],
            es: ["Walmart/Target", "Costco (mayoreo)", "Tiendas árabes/halal"],
          },
        },
        {
          title: { ar: "الطوارئ", en: "Emergencies", es: "Emergencias" },
          items: {
            ar: [
              "911 (شرطة/إسعاف/مطافئ)",
              "311 (خدمات بلدية)",
              "211 (خدمات اجتماعية)",
            ],
            en: [
              "911 (police/ambulance/fire)",
              "311 (city services)",
              "211 (social services)",
            ],
            es: [
              "911 (policía/ambulancia/bomberos)",
              "311 (servicios municipales)",
              "211 (servicios sociales)",
            ],
          },
        },
      ],
      tips: {
        ar: [
          "كوّن صداقات متنوعة لتندمج أسرع",
          "احترم القوانين والعادات المحلية",
        ],
        en: [
          "Make diverse friends to integrate faster",
          "Respect local laws and customs",
        ],
        es: [
          "Haz amigos diversos para integrarte rápido",
          "Respeta leyes y costumbres locales",
        ],
      },
    },
  },
};
