import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      status: 200,
      headers: corsHeaders,
    });
  }

  if (req.method !== "POST") {
    return new Response(
      JSON.stringify({ ok: false, reason: "method", version: "v3-debug" }),
      {
        status: 405,
        headers: {
          "Content-Type": "application/json",
          ...corsHeaders,
        },
      }
    );
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
    return new Response(
      JSON.stringify({ ok: false, reason: "config", version: "v3-debug" }),
      {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders,
      },
    }
    );
  }

  let payload: { email?: string };
  try {
    payload = await req.json();
  } catch {
    return new Response(JSON.stringify({ ok: false, reason: "json", version: "v3-debug" }), {
      status: 400,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders,
      },
    });
  }

  const email = (payload.email || "").trim().toLowerCase();
  if (!email) {
    return new Response(JSON.stringify({ ok: false, reason: "email_missing", version: "v3-debug" }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders,
      },
    });
  }

  try {
    console.log("Reset gate lookup for email:", email);
    const adminUrl = `${SUPABASE_URL}/auth/v1/admin/users`;
    const adminResponse = await fetch(adminUrl, {
      headers: {
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        apikey: SUPABASE_SERVICE_ROLE_KEY,
      },
    });

    if (!adminResponse.ok) {
      const detail = await adminResponse.text();
      console.error("Admin user lookup failed:", adminResponse.status, detail);
      return new Response(
        JSON.stringify({
          ok: false,
          reason: "user_lookup",
          version: "v4-debug",
          debug: {
            email,
            error: detail,
            supabase_url: SUPABASE_URL,
          },
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }

    const adminPayload = await adminResponse.json();
    const user = (adminPayload?.users || []).find(
      (entry: { email?: string }) =>
        (entry.email || "").trim().toLowerCase() === email
    );

    if (!user?.id) {
      return new Response(
        JSON.stringify({
          ok: false,
          reason: "user_not_found",
          version: "v4-debug",
          debug: {
            email,
            supabase_url: SUPABASE_URL,
          },
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            ...corsHeaders,
          },
        }
      );
    }

    const { data: subData, error: subError } = await supabase
      .from("paddle_subscriptions")
      .select("status")
      .eq("user_id", user.id)
      .maybeSingle();

    if (subError) {
      console.error("Subscription lookup error:", subError);
    }

    const status = (subData?.status || "").trim().toLowerCase();
    const ok =
      status === "active" || status === "trialing" || status === "complete";
    const reason = !subData
      ? "no_subscription"
      : ok
        ? "ok"
        : `inactive_status:${status || "unknown"}`;

    console.log("Reset gate status:", { userId: user.id, status, reason });
    return new Response(
      JSON.stringify({
        ok,
        reason,
        version: "v4-debug",
        debug: {
          email,
          user_id: user.id,
          subscription_user_id: subData?.user_id ?? null,
          subscription_status: subData?.status ?? null,
          subscription_found: Boolean(subData),
          supabase_url: SUPABASE_URL,
        },
      }),
      {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders,
      },
    }
    );
  } catch (error) {
    console.error("Reset gate error:", error);
    const message =
      error instanceof Error ? error.message : JSON.stringify(error);
    return new Response(JSON.stringify({ ok: false, reason: "exception", message, version: "v3-debug" }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        ...corsHeaders,
      },
    });
  }
});
