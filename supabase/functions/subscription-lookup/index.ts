import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { status: 200, headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ found: false }), {
      status: 405,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return new Response(JSON.stringify({ found: false }), {
      status: 200,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  let payload: { email?: string };
  try {
    payload = await req.json();
  } catch {
    return new Response(JSON.stringify({ found: false }), {
      status: 400,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  const email = (payload.email || "").trim().toLowerCase();
  if (!email) {
    return new Response(JSON.stringify({ found: false }), {
      status: 200,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  const adminResponse = await fetch(`${SUPABASE_URL}/auth/v1/admin/users`, {
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      apikey: SUPABASE_SERVICE_ROLE_KEY,
    },
  });

  if (!adminResponse.ok) {
    return new Response(JSON.stringify({ found: false }), {
      status: 200,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  const adminPayload = await adminResponse.json();
  const user = (adminPayload?.users || []).find(
    (entry: { email?: string }) =>
      (entry.email || "").trim().toLowerCase() === email
  );

  if (!user?.id) {
    return new Response(JSON.stringify({ found: false }), {
      status: 200,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }

  const { data: subData } = await supabase
    .from("paddle_subscriptions")
    .select("status")
    .eq("user_id", user.id)
    .maybeSingle();

  return new Response(
    JSON.stringify({
      found: true,
      user_id: user.id,
      status: subData?.status ?? null,
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    }
  );
});
