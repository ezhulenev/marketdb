package com.ergodicity.marketdb.iteratee

import scalaz.{Input, IterV}
import scalaz.IterV.{EOF, Done, Cont}

object MarketIteratees {
  def counter[E]: IterV[E, Int] = {
    def step(is: Int, e: E)(s: Input[E]): IterV[E, Int] = {
      s(el = e2 => Cont(step(is + 1, e2)),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Int] = {
      s(el = e1 => Cont(step(1, e1)),
        empty = Cont(first),
        eof = Done(0, EOF[E]))
    }

    Cont(first)
  }

  def sequencer[E]: IterV[E, Seq[E]] = {
    def step(is: Seq[E], e: E)(s: Input[E]): IterV[E, Seq[E]] = {
      s(el = e2 => Cont(step(is :+ e2, e2)),
        empty = Cont(step(is :+ e, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Seq[E]] = {
      s(el = e1 => Cont(step(Seq(e1), e1)),
        empty = Cont(first),
        eof = Done(Seq.empty[E], EOF[E]))
    }

    Cont(first)
  }

  def printer[E](log: org.slf4j.Logger): IterV[E, Boolean] = {
    def step(is: Boolean, e: E)(s: Input[E]): IterV[E, Boolean] = {
      log.info("STEP: " + e)
      s(el = e2 => Cont(step(is, e2)),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Boolean] = {
      s(el = e1 => Cont(step(true, e1)),
        empty = Cont(first),
        eof = Done(true, EOF[E]))
    }

    Cont(first)
  }
}
