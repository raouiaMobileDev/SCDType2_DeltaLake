package com.databeans.models

case class History(id: Integer, firstName:String, lastName: String, address:String, moved_in:String, moved_out:String, current: Boolean)
case class Update(id: Integer, firstName:String, lastName: String, address:String, moved_in:String)
case class LedMovedInAndLedAddress(id: Integer, firstName:String, lastName: String, address:String, moved_in:String, moved_out:String, current: Boolean, led_moved_in:String, led_address:String)
