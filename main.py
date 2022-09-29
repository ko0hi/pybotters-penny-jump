import asyncio
import sys
import pybotters

from argparse import ArgumentParser
from loguru import logger


async def main(args):
    # loguruのデフォルトのフォーマットはメッセージのスタートがズレて見にくいので修正
    logger.remove(0)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level:<8}</level> | "
        "<level>{message}</level>",
    )

    async with pybotters.Client(
        apis=args.api_key, base_url="https://api.bitflyer.com"
    ) as client:
        store = pybotters.bitFlyerDataStore()

        """  ########## HELPER FUNCTIONS ########## """
        # 引数で与えるの面倒なので関数内関数で済ませている
        # watch helper
        def watch(name: str, *params) -> asyncio.Task:
            """watcher builder。指定のwatcherをスケジューリングして返す。"""

            async def order(order_id: str):
                """注文watcher"""
                with store.childorderevents.watch() as stream:
                    async for msg in stream:
                        if (
                            msg.operation == "insert"
                            and msg.data["child_order_acceptance_id"] == order_id
                            and msg.data["event_type"]
                            in ("EXECUTION", "CANCEL", "ORDER_FAILED")
                        ):
                            return msg.data

            async def wall(side: str, price: int):
                """壁watcher

                閑話休題）pybottersのDataStoreのデータの持ち方：

                   - DataStoreごとにhash生成に使うkey-value pairsを定義している
                   - key-value pairsからhash値を生成
                   - hash値をkeyとするdictの中にデータを格納

                  この実装ではbitflyerのBoard DataStoreが使うkey-value pairsから事前に
                  hash値を計算しておいて、板情報を受信するたびにその値を確認しにいっている。
                  .findを使うとO(n)かかるが（nは板上の注文数）、これだとO(1)で取ってこれる。

                  .get({key-value pairs})というメソッドが用意されているが、これだと毎回hash値を
                  計算する必要があるので、ここでは事前に計算（対して変わらないとは思うが・・・）＋
                  隠しメソッド・隠し変数に直接アクセスして取得した。

                  また、watchを使う実装よりもこっちの方が速かった。watchを使う場合、

                  ①bfの1回のメッセージに含まれる板更新が全て終了する
                  ②板更新時に行ったinsert/delete情報を頭から流す

                  という感じになる、はず。②のところで関係ない更新情報まで舐めさせられる
                  （＋asyncioのスケジューラに他のtaskも存在すればそちらも間に挟まってくる）。
                  この実装では while True: await store.board.wait()で①を行って、その後に対象
                  となる注文部分を直接引っ張ってくる（＋その間に他のタスクも割り込ませない）ので速い。はず。

                """
                # bitflyerのBoardストアが使うkey-value pairs
                keys = {"product_code": args.symbol, "side": side, "price": price}
                # hash値を隠しメソッドで生成
                # 内部では``keys``をtupleにしてhashに渡している。ので、``keys``の中身の順番大事。
                hash_key = store.board._hash(keys)
                while True:
                    await store.board.wait()

                    # 存在しない（指値が消えた）
                    if hash_key not in store.board._index:
                        break

                    item = store.board._data[store.board._index[hash_key]]
                    # 存在するが注文量が下限を割った
                    if item["size"] <= args.t_exit:
                        break

            if name == "order":
                watcher_fn = order
            elif name == "wall":
                watcher_fn = wall
            else:
                raise RuntimeError(f"Unsupported: {name}")

            # 指定のwatcherをasyncio.Taskにして返す
            return asyncio.create_task(watcher_fn(*params))

        # 注文helper
        async def limit_order(side: str, size: float, price: int):
            res = await client.post(
                "/v1/me/sendchildorder",
                data={
                    "product_code": args.symbol,
                    "side": side,
                    "size": size,
                    "child_order_type": "LIMIT",
                    "price": int(price),
                },
            )
            data = await res.json()
            order_id = data["child_order_acceptance_id"]
            return order_id

        async def market_order(side: str, size: float, wait_execution: bool = False):
            res = await client.post(
                "/v1/me/sendchildorder",
                data={
                    "product_code": args.symbol,
                    "side": side,
                    "size": f"{size:.8f}",
                    "child_order_type": "MARKET",
                },
            )
            data = await res.json()
            if wait_execution:
                # 損益計算用に約定情報を待機する
                watcher = watch("order", data["child_order_acceptance_id"])
                await watcher
                return watcher.result()
            else:
                return data["child_order_acceptance_id"]

        async def cancel_order(order_id: str):
            order_id_key = "child_order_id"
            if order_id.startswith("JRF"):
                order_id_key = order_id_key.replace("_id", "_acceptance_id")
            res = await client.post(
                "/v1/me/cancelchildorder",
                data={"product_code": args.symbol, order_id_key: order_id},
            )
            return res.status == 200

        def best_price(side: str, margin: int = 1):
            """最良気配値＋α"""
            board = store.board.sorted({"side": side})
            return board[side][0]["price"] + (margin if side == "BUY" else -margin)

        def compute_pnl(entry_result: dict, exit_result: dict, side: str):
            pnl = exit_result["price"] - entry_result["price"]
            if side == "SELL":
                pnl *= -1
            return pnl

        # logic
        async def penny_jump(side: str, price: int):
            if side == "BUY":
                entry_price = price + args.margin
                exit_side = "SELL"
            else:
                entry_price = price - args.margin
                exit_side = "BUY"

            # 逆ポジションを持っていればその分を新規注文で打ち消す（簡易部分約定対策）
            entry_size = args.size + sum(
                [x["size"] for x in store.positions.find({"side": exit_side})]
            )

            logger.debug(
                f"start penny jump: side={side} wall={price:.0f} entry={entry_price:.0f}"
            )

            # 壁監視タスク
            wall_watcher = watch("wall", side, price)

            # リードタイム分待機
            done, pending = await asyncio.wait([wall_watcher], timeout=args.lead_time)
            if done:
                # 壁がすでにないのでエントリーせずに終了
                logger.debug(f"the wall already disappeared before ordering. next.")
                wall_watcher.cancel()
                return None, None

            # エントリー
            entry_order_id = await limit_order(side, entry_size, entry_price)
            entry_watcher = watch("order", entry_order_id)

            async def _cancel_otherwise_market():
                # 壁監視はもう不要なのでキャンセル
                wall_watcher.cancel()

                # 注文取消＋失敗時に成行決済
                await cancel_order(entry_order_id)
                # entry_watcherも再利用して取消イベントの受信待機
                if not entry_watcher.done():
                    await entry_watcher

                entry_res = entry_watcher.result()
                if entry_res["event_type"] == "CANCEL":
                    return None, None
                else:
                    # 取消失敗（この間に約定した）
                    # 壁はすでになくなっているので急いで成行決済
                    exit_res = await market_order(
                        exit_side, args.size, wait_execution=True
                    )
                    return entry_res, exit_res

            # 注文約定 or 壁消失待機
            done, pending = await asyncio.wait(
                [entry_watcher, wall_watcher],
                timeout=args.expire_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # 時間切れ
            if len(done) == 0:
                logger.debug("timeout")
                return await _cancel_otherwise_market()

            if not entry_watcher.done():
                # entry_watcherが終了していない
                # ＝> wall_watcherは終了している
                # => 壁消失 ＋　注文未約定
                # => 注文キャンセル
                return await _cancel_otherwise_market()

            logger.debug(f"success entry. go exit.")

            # entry_watcherが終了している = 約定 = 最良気配値で決済注文
            exit_order_id = await limit_order(
                exit_side, args.size, best_price(exit_side)
            )
            exit_watcher = watch("order", exit_order_id)

            # 注文約定 or 壁消失待機
            await asyncio.wait(
                [exit_watcher, wall_watcher], return_when=asyncio.FIRST_COMPLETED
            )

            # watchタスクが残らないように掃除
            entry_watcher.cancel()
            exit_watcher.cancel()
            wall_watcher.cancel()

            if exit_watcher.done():
                # 決済指値約定
                return entry_watcher.result(), exit_watcher.result()
            else:
                # 壁崩壊寸前＋決済指値未約定 = 指値取消＋成行逃走
                # キャンセルを確認してからmarket_orderすると二重約定を防げる。が、そのうちに壁がなく
                # なってしまうかもしれないのでここではキャンセルと成行を同時に出している。
                _, exit_result = await asyncio.gather(
                    cancel_order(exit_order_id),
                    market_order(exit_side, args.size, wait_execution=True),
                )
                return entry_watcher.result(), exit_result
        """  ########## HELPER FUNCTIONS ########## """

        await client.ws_connect(
            "wss://ws.lightstream.bitflyer.com/json-rpc",
            send_json=[
                {
                    "method": "subscribe",
                    "params": {"channel": "lightning_board_snapshot_FX_BTC_JPY"},
                    "id": 1,
                },
                {
                    "method": "subscribe",
                    "params": {"channel": "lightning_board_FX_BTC_JPY"},
                    "id": 2,
                },
                {
                    "method": "subscribe",
                    "params": {"channel": "child_order_events"},
                    "id": 3,
                },
            ],
            hdlr_json=store.onmessage,
        )

        while not all([len(w) for w in [store.board]]):
            logger.debug("waiting socket response")
            await store.wait()

        with store.board.watch() as stream:
            async for msg in stream:
                if msg.operation == "insert":
                    d = msg.data
                    best = best_price(d["side"], 0)
                    if (
                        # 注文サイズが壁の閾値を超えている
                        d["size"] >= args.t_entry
                        # 最良気配値からmin_dist以上・max_dist以下の距離にある
                        and args.min_dist <= abs(d["price"] - best) <= args.max_dist
                    ):
                        entry_result, exit_result = await penny_jump(
                            d["side"], d["price"]
                        )

                        if entry_result and exit_result:
                            pnl = compute_pnl(entry_result, exit_result, d["side"])
                            fn = logger.success if pnl > 0 else logger.error
                            fn(f"[RESULT] {'win' if pnl > 0 else 'lose'} {pnl}")
                        else:
                            logger.info("[RESULT] draw")

                        # 処理中配信された”古い”板情報がキューに溜まっているので全部取り出す
                        while stream._queue.qsize():
                            stream._queue.get_nowait()


if __name__ == "__main__":
    parser = ArgumentParser(description="pybotters x asyncio x penny jump")
    parser.add_argument("--api_key", help="apiキー", default="../apis.json")
    parser.add_argument("--symbol", default="FX_BTC_JPY", help="取引通貨")
    parser.add_argument("--size", default=0.01, type=float, help="注文サイズ")
    parser.add_argument("--t_entry", default=0.5, type=float, help="壁の閾値")
    parser.add_argument("--t_exit", default=0.25, type=float, help="壁の下限")
    parser.add_argument("--margin", default=200, type=float, help="壁を起点とした指値位置")
    parser.add_argument("--max_dist", default=2000, type=int, help="最良気配値からの最大距離")
    parser.add_argument("--min_dist", default=0, type=int, help="最良気配値からの最小距離")
    parser.add_argument("--lead_time", default=1, type=int, help="新規注文を出すまでのリードタイム")
    parser.add_argument("--expire_seconds", default=10, type=int, help="１トライの時間制限")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt as e:
        pass
