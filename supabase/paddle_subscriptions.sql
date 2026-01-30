create table if not exists public.paddle_subscriptions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  paddle_customer_id text,
  paddle_subscription_id text,
  status text,
  price_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists paddle_subscriptions_user_id_key
  on public.paddle_subscriptions(user_id);

alter table public.paddle_subscriptions enable row level security;

create policy "read_own_subscription"
  on public.paddle_subscriptions
  for select
  using (auth.uid() = user_id);
