import { Subscription } from '@atproto/xrpc-server'
import { cborToLexRecord, readCar } from '@atproto/repo'
import { BlobRef } from '@atproto/lexicon'
import { ids, lexicons } from '../lexicon/lexicons'
import { Record as PostRecord } from '../lexicon/types/app/bsky/feed/post'
import { Record as RepostRecord } from '../lexicon/types/app/bsky/feed/repost'
import { Record as LikeRecord } from '../lexicon/types/app/bsky/feed/like'
import { Record as FollowRecord } from '../lexicon/types/app/bsky/graph/follow'
import {
  Commit,
  OutputSchema as RepoEvent,
  isCommit,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { Database } from '../db'
import { AtpAgent } from '@atproto/api'
import dotenv from 'dotenv'
import * as fs from 'node:fs/promises'

let authorFollowersCount = {}

const syncAuthorFollowers = async () => {
  try {
    const result = await fs.readFile('authorFollowers.json', 'utf8')
    authorFollowersCount = JSON.parse(result)
    console.log(`synced ${Object.keys(authorFollowersCount).length} authors`)
  } catch {
    console.log(`DANG IT`)
    authorFollowersCount = {}
  }
}

export abstract class FirehoseSubscriptionBase {
  public sub: Subscription<RepoEvent>

  constructor(public db: Database, public service: string) {
    this.sub = new Subscription({
      service: service,
      method: ids.ComAtprotoSyncSubscribeRepos,
      getParams: () => this.getCursor(),
      validate: (value: unknown) => {
        try {
          return lexicons.assertValidXrpcMessage<RepoEvent>(
            ids.ComAtprotoSyncSubscribeRepos,
            value,
          )
        } catch (err) {
          console.error('repo subscription skipped invalid message', err)
        }
      },
    })
  }

  abstract handleEvent(
    evt: RepoEvent,
    agent,
    MAX_THRESHOLD,
    MIN_THRESHOLD,
    MIN_AGE_OF_POST_IN_MS,
    MAX_AGE_OF_POST_IN_MS,
    authorFollowersCount,
    maxFollowersAllowed,
    syncAuthorFollowers,
  ): Promise<void>

  async run() {
    dotenv.config()

    // const handle = process.env.BLUESKY_HANDLE!
    // const password = process.env.BLUESKY_PASSWORD!

    const MAX_THRESHOLD = process.env.BLUESKY_MAX_THRESHOLD!
    const MIN_THRESHOLD = process.env.BLUESKY_MIN_THRESHOLD!
    const MIN_AGE_OF_POST_IN_MS = process.env.BLUESKY_MIN_AGE_OF_POST_IN_MS!
    const MAX_AGE_OF_POST_IN_MS = process.env.BLUESKY_MAX_AGE_OF_POST_IN_MS!
    const maxFollowersAllowed = process.env.BLUESKY_MAX_FOLLOWERS_ALLOWED!

    await syncAuthorFollowers()

    const timeFormat = {
      month: 'numeric',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZoneName: 'short',
      timeZone: 'America/Los_Angeles',
    } as const
    const formatter = new Intl.DateTimeFormat([], timeFormat)

    console.log(formatter.format(new Date()))
    console.log(`MAX_THRESHOLD: ${MAX_THRESHOLD}`)
    console.log(`MIN_THRESHOLD: ${MIN_THRESHOLD}`)
    console.log(`MIN_AGE_OF_POST_IN_MS: ${MIN_AGE_OF_POST_IN_MS}`)
    console.log(`MAX_AGE_OF_POST_IN_MS: ${MAX_AGE_OF_POST_IN_MS}`)

    // const agent = new AtpAgent({ service: 'https://bsky.social' })
    // await agent.login({ identifier: handle, password })

    for await (const evt of this.sub) {
      try {
        await this.handleEvent(
          evt,
          undefined,
          MAX_THRESHOLD,
          MIN_THRESHOLD,
          MIN_AGE_OF_POST_IN_MS,
          MAX_AGE_OF_POST_IN_MS,
          authorFollowersCount,
          maxFollowersAllowed,
          syncAuthorFollowers,
        )
      } catch (err) {
        console.error('repo subscription could not handle message', err)
      }
      // update stored cursor every 20 events or so
      if (isCommit(evt) && evt.seq % 20 === 0) {
        await this.updateCursor(evt.seq)
      }
    }
  }

  async updateCursor(cursor: number) {
    await this.db
      .updateTable('sub_state')
      .set({ cursor })
      .where('service', '=', this.service)
      .execute()
  }

  async getCursor(): Promise<{ cursor?: number }> {
    const res = await this.db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', this.service)
      .executeTakeFirst()
    return res ? { cursor: res.cursor } : {}
  }
}

export const getOpsByType = async (evt: Commit): Promise<OperationsByType> => {
  const car = await readCar(evt.blocks)
  const opsByType: OperationsByType = {
    posts: { creates: [], deletes: [] },
    reposts: { creates: [], deletes: [] },
    likes: { creates: [], deletes: [] },
    follows: { creates: [], deletes: [] },
  }

  for (const op of evt.ops) {
    const uri = `at://${evt.repo}/${op.path}`
    const [collection] = op.path.split('/')

    if (op.action === 'update') continue // updates not supported yet

    if (op.action === 'create') {
      if (!op.cid) continue
      const recordBytes = car.blocks.get(op.cid)
      if (!recordBytes) continue
      const record = cborToLexRecord(recordBytes)
      const create = { uri, cid: op.cid.toString(), author: evt.repo }
      if (collection === ids.AppBskyFeedPost && isPost(record)) {
        opsByType.posts.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyFeedRepost && isRepost(record)) {
        opsByType.reposts.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyFeedLike && isLike(record)) {
        opsByType.likes.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyGraphFollow && isFollow(record)) {
        opsByType.follows.creates.push({ record, ...create })
      }
    }

    if (op.action === 'delete') {
      if (collection === ids.AppBskyFeedPost) {
        opsByType.posts.deletes.push({ uri })
      } else if (collection === ids.AppBskyFeedRepost) {
        opsByType.reposts.deletes.push({ uri })
      } else if (collection === ids.AppBskyFeedLike) {
        opsByType.likes.deletes.push({ uri })
      } else if (collection === ids.AppBskyGraphFollow) {
        opsByType.follows.deletes.push({ uri })
      }
    }
  }

  return opsByType
}

type OperationsByType = {
  posts: Operations<PostRecord>
  reposts: Operations<RepostRecord>
  likes: Operations<LikeRecord>
  follows: Operations<FollowRecord>
}

type Operations<T = Record<string, unknown>> = {
  creates: CreateOp<T>[]
  deletes: DeleteOp[]
}

type CreateOp<T> = {
  uri: string
  cid: string
  author: string
  record: T
}

type DeleteOp = {
  uri: string
}

export const isPost = (obj: unknown): obj is PostRecord => {
  return isType(obj, ids.AppBskyFeedPost)
}

export const isRepost = (obj: unknown): obj is RepostRecord => {
  return isType(obj, ids.AppBskyFeedRepost)
}

export const isLike = (obj: unknown): obj is LikeRecord => {
  return isType(obj, ids.AppBskyFeedLike)
}

export const isFollow = (obj: unknown): obj is FollowRecord => {
  return isType(obj, ids.AppBskyGraphFollow)
}

const isType = (obj: unknown, nsid: string) => {
  try {
    lexicons.assertValidRecord(nsid, fixBlobRefs(obj))
    return true
  } catch (err) {
    return false
  }
}

// @TODO right now record validation fails on BlobRefs
// simply because multiple packages have their own copy
// of the BlobRef class, causing instanceof checks to fail.
// This is a temporary solution.
const fixBlobRefs = (obj: unknown): unknown => {
  if (Array.isArray(obj)) {
    return obj.map(fixBlobRefs)
  }
  if (obj && typeof obj === 'object') {
    if (obj.constructor.name === 'BlobRef') {
      const blob = obj as BlobRef
      return new BlobRef(blob.ref, blob.mimeType, blob.size, blob.original)
    }
    return Object.entries(obj).reduce((acc, [key, val]) => {
      return Object.assign(acc, { [key]: fixBlobRefs(val) })
    }, {} as Record<string, unknown>)
  }
  return obj
}
