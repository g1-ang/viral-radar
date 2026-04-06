import { Actor } from 'apify';
import { createClient } from '@supabase/supabase-js';

const VIRAL_RULES = [
  { maxHours: 6,  minGrowthRate: 200, minViews: 1_000 },
  { maxHours: 12, minGrowthRate: 200, minViews: 3_000 },
  { maxHours: 24, minGrowthRate: 300, minViews: 7_000 },
];

const RECHECK_WINDOW_HOURS = 24;
const MAX_POST_AGE_DAYS = 7; // ✅ 7일 이내 게시물만 수집
const MAX_SNAPSHOTS = 3;     // ✅ 최대 재측정 횟수 (비용 절감)
const MIN_VIEWS_FOR_RECHECK = 100; // ✅ 초기 100뷰 이하는 재측정 스킵

const BRANDS = [
  {
    key: 'meitu',
    label: '메이투',
    hashtags: ['meitu', '메이투'],
    keywords: ['meitu', '메이투', '메이튜', 'meitu보정'],
    accountIds: ['meitu.kr', 'meitu.app', 'meituapp'],
  },
  {
    key: 'beautycam',
    label: '뷰티캠',
    hashtags: ['beautycam', '뷰티캠'],
    keywords: ['beautycam', '뷰티캠'],
    accountIds: ['beautycam.kr', 'beautycam.app', 'beautycam.vn'],
  },
];

function checkViralCondition(growthRate, hoursElapsed, currentViews) {
  for (const rule of VIRAL_RULES) {
    if (
      hoursElapsed <= rule.maxHours &&
      growthRate >= rule.minGrowthRate &&
      currentViews >= rule.minViews
    ) {
      return true;
    }
  }
  return false;
}

function hasKorean(text) {
  return /[\uAC00-\uD7A3]/.test(text || '');
}

function isKoreanBrandPost(post, brand) {
  const text = [
    post.caption || '',
    (post.hashtags || []).join(' '),
    (post.mentions || []).join(' '),
  ].join(' ');

  const taggedUsernames = (post.taggedUsers || []).map(u =>
    (u.username || '').toLowerCase()
  );
  const isTagged = brand.accountIds.some(id =>
    taggedUsernames.some(u => u.includes(id))
  );

  const hasKoreanText = hasKorean(text);
  if (!hasKoreanText && !isTagged) return false;

  const keywords = brand.keywords.map(k => k.toLowerCase());
  const hasKeyword = keywords.some(kw => text.toLowerCase().includes(kw));

  return hasKeyword || isTagged;
}

function safeTimestamp(ts) {
  if (!ts) return null;
  try {
    const num = Number(ts);
    if (!isNaN(num) && num > 0) {
      const d = new Date(num * 1000);
      if (!isNaN(d.getTime())) return d.toISOString();
    }
    const d2 = new Date(ts);
    if (!isNaN(d2.getTime())) return d2.toISOString();
    return null;
  } catch (_) {
    return null;
  }
}

function extractMetrics(post) {
  const views    = post.videoViewCount || post.videoPlayCount || post.playsCount || post.igPlayCount || 0;
  const likes    = post.likesCount || post.likeCount || post.likes || 0;
  const comments = post.commentsCount || post.commentCount || post.comments || 0;
  return { views, likes, comments };
}

// ✅ 최근 N일 이내 게시물인지 확인
function isRecentPost(post) {
  const ts = post.timestamp;
  if (!ts) return false; // ✅ timestamp 없으면 오래된 것으로 간주하고 제외

  let postDate;
  const num = Number(ts);
  if (!isNaN(num) && num > 0) {
    // Unix timestamp (초 단위) vs 밀리초 자동 판별
    postDate = new Date(num > 1e12 ? num : num * 1000);
  } else {
    postDate = new Date(ts);
  }

  if (isNaN(postDate.getTime())) return false; // 파싱 실패 시 제외

  const cutoff = new Date(Date.now() - MAX_POST_AGE_DAYS * 24 * 60 * 60 * 1000);
  return postDate >= cutoff;
}

async function trackA_newCollection(supabase, brand, maxPosts) {
  console.log(`[${brand.key}][트랙A] 릴스 수집 시작 — ${brand.hashtags.join(', ')}`);

  const run = await Actor.call('apify/instagram-hashtag-scraper', {
    hashtags:      brand.hashtags,
    resultsLimit:  30,
    resultsType:   'reels',
    searchType:    'recent',  // ✅ 최신순 (Top → Recent)
    keywordSearch: false,
    proxy: { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
  });

  const { items } = await Actor.openDataset(run.defaultDatasetId)
    .then(ds => ds.getData());

  const brandReels = items.filter(p => isKoreanBrandPost(p, brand));
  // ✅ 최근 7일 이내 게시물만 필터링
  const recentReels = brandReels.filter(p => isRecentPost(p));

  console.log(`[${brand.key}][트랙A] 전체 ${items.length}개 → 브랜드 릴스 ${brandReels.length}개 → 최근 7일 ${recentReels.length}개`);

  const latest = recentReels
    .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
    .slice(0, maxPosts);

  let newCount = 0;
  const updatedPostIds = new Set(); // ✅ TrackA에서 업데이트한 기존 게시물 ID

  for (const post of latest) {
    const postId    = post.id || post.shortCode;
    const { views, likes, comments } = extractMetrics(post);
    const shortCode = post.shortCode || null;
    const postUrl   = post.url || (shortCode ? `https://www.instagram.com/reel/${shortCode}/` : null);

    if (!postUrl) continue;

    let existing = null;
    try {
      const { data } = await supabase
        .from('posts')
        .select('id')
        .eq('post_id', postId)
        .single();
      existing = data;
    } catch (_) {}

    if (existing) {
      // ✅ 이미 수집된 게시물이면 스냅샷 업데이트 (TrackB 호출 절약)
      if (views > 0) {
        let lastSnap = null;
        try {
          const { data } = await supabase
            .from('post_snapshots')
            .select('views, snapshot_seq')
            .eq('post_id', existing.id)
            .order('snapshot_seq', { ascending: false })
            .limit(1)
            .single();
          lastSnap = data;
        } catch (_) {}

        if (lastSnap && lastSnap.snapshot_seq < MAX_SNAPSHOTS && lastSnap.views > 0) {
          const prevViews = lastSnap.views;
          const viewsDelta = views - prevViews;
          const growthRate = ((views - prevViews) / prevViews) * 100;

          const { data: postData } = await supabase
            .from('posts')
            .select('first_seen_at')
            .eq('id', existing.id)
            .single();
          const hoursElapsed = postData
            ? (Date.now() - new Date(postData.first_seen_at)) / 3_600_000
            : 0;

          await supabase.from('post_snapshots').insert({
            post_id:           existing.id,
            instagram_post_id: postId,
            views,
            likes,
            comments,
            views_delta:       viewsDelta,
            views_growth_rate: Math.round(growthRate * 10) / 10,
            hours_elapsed:     Math.round(hoursElapsed * 10) / 10,
            snapshot_seq:      lastSnap.snapshot_seq + 1,
          });

          // 바이럴 체크
          if (checkViralCondition(growthRate, hoursElapsed, views)) {
            await supabase
              .from('posts')
              .update({ is_viral: true, viral_detected_at: new Date().toISOString() })
              .eq('id', existing.id);
            console.log(`[${brand.key}][트랙A] 🔥 재활용 바이럴 감지! ${postId} +${growthRate.toFixed(1)}%`);
          } else {
            console.log(`[${brand.key}][트랙A] 재활용 업데이트: ${postId} +${growthRate.toFixed(1)}%`);
          }
          updatedPostIds.add(postId);
        }
      }
      continue;
    }

    const { data: saved, error } = await supabase
      .from('posts')
      .insert({
        post_id:          postId,
        brand:            brand.key,
        url:              postUrl,
        caption:          post.caption?.slice(0, 2000) || null,
        thumbnail_url:    post.displayUrl || post.thumbnailUrl || null,
        hashtags:         (post.hashtags || []),
        owner_username:   post.ownerUsername || null,
        follower_count:   post.ownerFollowersCount || 0,
        initial_views:    views,
        initial_likes:    likes,
        initial_comments: comments,
        posted_at:        safeTimestamp(post.timestamp),
      })
      .select('id')
      .single();

    if (error) {
      console.error(`[${brand.key}][트랙A] 저장 오류:`, error.message);
      continue;
    }

    if (views > 0) {
      await supabase.from('post_snapshots').insert({
        post_id:            saved.id,
        instagram_post_id:  postId,
        views,
        likes,
        comments,
        views_delta:        0,
        views_growth_rate:  0,
        hours_elapsed:      0,
        snapshot_seq:       1,
      });
    } else {
      console.log(`[${brand.key}][트랙A] views=0, seq1 보류: ${postId}`);
    }

    newCount++;
    console.log(`[${brand.key}][트랙A] 저장: ${postId} | 조회수: ${views.toLocaleString()}`);
  }

  console.log(`[${brand.key}][트랙A] 완료 — ${newCount}개 신규 저장, ${updatedPostIds.size}개 재활용 업데이트`);
  return { newCount, updatedPostIds };
}

async function trackB_recheck(supabase, brand, slackWebhook, updatedByTrackA = new Set()) {
  console.log(`[${brand.key}][트랙B] 재측정 시작`);

  const windowStart = new Date(Date.now() - RECHECK_WINDOW_HOURS * 60 * 60 * 1000).toISOString();

  const { data: allTargets, error } = await supabase
    .from('posts')
    .select('id, post_id, url, initial_views, first_seen_at')
    .eq('brand', brand.key)
    .eq('is_viral', false)
    .gte('first_seen_at', windowStart)
    .order('first_seen_at', { ascending: false });

  if (error || !allTargets?.length) {
    console.log(`[${brand.key}][트랙B] 재측정 대상 없음`);
    return 0;
  }

  // ✅ 최소 조회수 필터 + 최대 재측정 횟수 제한 + TrackA 재활용 제외
  const targets = [];
  for (const t of allTargets) {
    if (t.initial_views < MIN_VIEWS_FOR_RECHECK) {
      console.log(`[${brand.key}][트랙B] 조회수 부족 스킵 (${t.initial_views}뷰): ${t.post_id}`);
      continue;
    }
    // 이미 TrackA에서 업데이트된 게시물은 제외
    if (updatedByTrackA.has(t.post_id)) {
      console.log(`[${brand.key}][트랙B] TrackA 재활용 스킵: ${t.post_id}`);
      continue;
    }
    let lastSnap = null;
    try {
      const { data } = await supabase
        .from('post_snapshots')
        .select('snapshot_seq')
        .eq('post_id', t.id)
        .order('snapshot_seq', { ascending: false })
        .limit(1)
        .single();
      lastSnap = data;
    } catch (_) {}
    if (lastSnap && lastSnap.snapshot_seq >= MAX_SNAPSHOTS) {
      console.log(`[${brand.key}][트랙B] 측정 완료 스킵 (${lastSnap.snapshot_seq}회): ${t.post_id}`);
      continue;
    }
    targets.push(t);
  }

  if (!targets.length) {
    console.log(`[${brand.key}][트랙B] 필터 후 재측정 대상 없음 (전체 ${allTargets.length}개 중)`);
    return 0;
  }

  console.log(`[${brand.key}][트랙B] 재측정 대상: ${targets.length}개 (전체 ${allTargets.length}개 중)`);
  let viralFound = 0;

  for (const target of targets) {
    try {
      if (!target.url) continue;

      const run = await Actor.call('apify/instagram-post-scraper', {
        username:     [target.url],
        resultsLimit: 1,
        proxy: { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
      });

      const { items } = await Actor.openDataset(run.defaultDatasetId)
        .then(ds => ds.getData());

      if (!items?.length) {
        console.log(`[${brand.key}][트랙B] 결과 없음: ${target.post_id}`);
        continue;
      }

      const fresh = items[0];
      const { views: currentViews, likes: currentLikes, comments: currentComments } = extractMetrics(fresh);

      let lastSnap = null;
      try {
        const { data } = await supabase
          .from('post_snapshots')
          .select('views, snapped_at, snapshot_seq')
          .eq('instagram_post_id', target.post_id)
          .order('snapped_at', { ascending: false })
          .limit(1)
          .single();
        lastSnap = data;
      } catch (_) {}

      if (!lastSnap) {
        if (currentViews > 0) {
          await supabase.from('post_snapshots').insert({
            post_id:           target.id,
            instagram_post_id: target.post_id,
            views:             currentViews,
            likes:             currentLikes,
            comments:          currentComments,
            views_delta:       0,
            views_growth_rate: 0,
            hours_elapsed:     0,
            snapshot_seq:      1,
          });
          console.log(`[${brand.key}][트랙B] seq1 저장: ${target.post_id} (views=${currentViews})`);
        }
        continue;
      }

      const prevViews = lastSnap.views || 0;
      if (prevViews === 0) {
        console.log(`[${brand.key}][트랙B] prevViews=0 스킵: ${target.post_id}`);
        continue;
      }

      const hoursElapsed = (Date.now() - new Date(target.first_seen_at)) / 3_600_000;
      const viewsDelta   = currentViews - prevViews;
      const growthRate   = ((currentViews - prevViews) / prevViews) * 100;
      const nextSeq      = lastSnap.snapshot_seq + 1;

      await supabase.from('post_snapshots').insert({
        post_id:           target.id,
        instagram_post_id: target.post_id,
        views:             currentViews,
        likes:             currentLikes,
        comments:          currentComments,
        views_delta:       viewsDelta,
        views_growth_rate: Math.round(growthRate * 10) / 10,
        hours_elapsed:     Math.round(hoursElapsed * 10) / 10,
        snapshot_seq:      nextSeq,
      });

      console.log(
        `[${brand.key}][트랙B] ${target.post_id} | ` +
        `조회수: ${currentViews.toLocaleString()} | +${growthRate.toFixed(1)}% | ${hoursElapsed.toFixed(1)}h`
      );

      const isViral = checkViralCondition(growthRate, hoursElapsed, currentViews);

      if (isViral) {
        viralFound++;
        console.log(`[${brand.key}][트랙B] 🔥 바이럴 감지! ${target.post_id}`);

        await supabase
          .from('posts')
          .update({ is_viral: true, viral_detected_at: new Date().toISOString() })
          .eq('id', target.id);

        await supabase.from('viral_alerts').insert({
          post_id:             target.id,
          brand:               brand.key,
          views_at_detection:  currentViews,
          views_growth_rate:   Math.round(growthRate * 10) / 10,
          hours_elapsed:       Math.round(hoursElapsed * 10) / 10,
          snapshot_seq:        nextSeq,
        });

        if (slackWebhook) {
          await sendSlackAlert(slackWebhook, brand, {
            url:       target.url,
            caption:   fresh.caption,
            thumbnail: fresh.displayUrl || fresh.thumbnailUrl,
          }, {
            currentViews,
            currentLikes,
            currentComments,
            growthRate:   Math.round(growthRate * 10) / 10,
            hoursElapsed: Math.round(hoursElapsed * 10) / 10,
            viewsDelta,
            snapshotSeq:  nextSeq,
          });
        }
      }

    } catch (err) {
      console.warn(`[${brand.key}][트랙B] 재측정 실패 (${target.post_id}):`, err.message);
    }
  }

  console.log(`[${brand.key}][트랙B] 완료 — ${viralFound}개 바이럴 감지`);
  return viralFound;
}

async function sendSlackAlert(webhookUrl, brand, post, stats) {
  const body = {
    blocks: [
      {
        type: 'header',
        text: { type: 'plain_text', text: `🔥 릴스 급상승 감지 — ${brand.label}`, emoji: true },
      },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `*조회수 증가율*\n+${stats.growthRate.toLocaleString()}%` },
          { type: 'mrkdwn', text: `*현재 조회수*\n${stats.currentViews.toLocaleString()}` },
          { type: 'mrkdwn', text: `*증가량*\n+${stats.viewsDelta.toLocaleString()}` },
          { type: 'mrkdwn', text: `*좋아요*\n${stats.currentLikes.toLocaleString()}` },
          { type: 'mrkdwn', text: `*댓글*\n${stats.currentComments.toLocaleString()}` },
          { type: 'mrkdwn', text: `*경과 시간*\n${stats.hoursElapsed}h 만에 감지` },
        ],
      },
      ...(post.caption ? [{
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `*캡션*\n${post.caption.slice(0, 200)}${post.caption.length > 200 ? '...' : ''}`,
        },
      }] : []),
      {
        type: 'actions',
        elements: [{
          type: 'button',
          text: { type: 'plain_text', text: '🎬 릴스 보러가기', emoji: true },
          url: post.url,
          style: 'primary',
        }],
      },
    ],
  };

  const res = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!res.ok) throw new Error(`Slack webhook failed: ${res.status}`);
}

Actor.main(async () => {
  const input = await Actor.getInput();

  const SUPABASE_URL  = input?.supabaseUrl  || process.env.SUPABASE_URL;
  const SUPABASE_KEY  = input?.supabaseKey  || process.env.SUPABASE_SERVICE_KEY;
  const SLACK_WEBHOOK = input?.slackWebhook || process.env.SLACK_WEBHOOK_URL;
  const MAX_POSTS     = input?.maxPosts     || 5;

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

  for (const brand of BRANDS) {
    console.log(`\n[${brand.key}] ========== 시작 ==========`);
    let newCount   = 0;
    let viralFound = 0;

    try {
      const trackAResult = await trackA_newCollection(supabase, brand, MAX_POSTS);
      newCount = trackAResult.newCount;
      viralFound = await trackB_recheck(supabase, brand, SLACK_WEBHOOK, trackAResult.updatedPostIds);

      await supabase.from('crawl_logs').insert({
        brand:           brand.key,
        track:           'both',
        status:          'success',
        posts_found:     newCount,
        posts_rechecked: MAX_POSTS,
        viral_found:     viralFound,
      });

    } catch (err) {
      console.error(`[${brand.key}] 오류:`, err.message);
      await supabase.from('crawl_logs').insert({
        brand:     brand.key,
        track:     'both',
        status:    'error',
        error_msg: err.message,
      });
    }
  }
});
