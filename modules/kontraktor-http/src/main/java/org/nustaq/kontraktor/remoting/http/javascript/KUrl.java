package org.nustaq.kontraktor.remoting.http.javascript;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by ruedi on 19/07/15.
 *
 * url wrapper. note '//www.x.y' style not supported
 *
 * FIXME: replace/subclass with HttpUrl from okhttp lib
 *
 */
public class KUrl implements Serializable {

    String protocol;
    String elements[];

    boolean isTopDomain = false; // "/images/bla.png"
    boolean isTopRoot = false; // "//x.y.com/images"
    public static String stripDoubleSeps(String url) {
        while( url.indexOf("//") > 0 )
            url = url.replace("//","/");
        return url;
    }

    public KUrl( String url ) {
        int idx = url.indexOf("://");
        if ( idx >= 0 ) {
            protocol = url.substring(0,idx);
            url = url.substring(idx+3);
        }
        isTopRoot = url.startsWith("//");
        if (!isTopRoot) {
            isTopDomain = url.startsWith("/");
        }
        url = stripDoubleSeps(url);
        elements = url.split("/");
        normalize();
    }

    public KUrl(String protocol, String[] elements) {
        this.protocol = protocol;
        this.elements = elements;
    }

    public KUrl(String[] elements) {
        this.elements = elements;
    }

    protected void normalize() {
        List<String> newElems = new ArrayList<>();
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i].trim();//.toLowerCase(); is significant (feedburner)
            if ( element.length() > 0 ) {
                if ( ! element.equals(".") ) {
                    if ( element.equals("..") && newElems.size() > 0 && ! "..".equalsIgnoreCase(newElems.get(newElems.size()-1)) ) {
                        newElems.remove(newElems.size()-1);
                    } else {
                        newElems.add(element);
                    }
                }
            }
        }
        elements = new String[newElems.size()];
        newElems.toArray(elements);
    }

    public KUrl concat(String url) {
        return concat(new KUrl(url));
    }

    public KUrl concat(KUrl url) {
        String[] elems = url.getElements();
        if (url.isTopRoot) {
            return new KUrl(protocol,elems);
        }
        if (url.isTopDomain) {
            String newElems[] = new String[elems.length+1];
            System.arraycopy(elems,0,newElems,1,elems.length);
            newElems[0] = elements[0];
            return new KUrl(protocol,newElems);
        }
        String newElems[] = new String[elems.length+elements.length];
        System.arraycopy(elements,0,newElems,0,elements.length);
        System.arraycopy(elems, 0, newElems, elements.length, elems.length);
        KUrl res = new KUrl(protocol,newElems);
        res.normalize();
        return res;
    }

    public String getProtocol() {
        return protocol;
    }

    public String[] getElements() {
        return elements;
    }

    public String getExtension() {
        String name = getName();
        int idx = name.lastIndexOf(".");
        if ( idx >= 0 ) {
            return name.substring(idx+1);
        } else
            return "";
    }

    public String getFileNameNoExtension() {
        String name = getName();
        int idx = name.lastIndexOf(".");
        if ( idx >= 0 ) {
            name = name.substring(0,idx);
        }
        return name;
    }

    public String toUrlString() {
        return toUrlString(true);
    }

    public String mangled() {
        return mangled(true);
    }

    public String mangled(boolean allowFileSep) {
        String s = toUrlString(false);
        StringBuilder res = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
             char c = s.charAt(i);
             if ( c > 127 || (! Character.isLetterOrDigit(c) && (c != '/' || ! allowFileSep) && c != '.' && c != '-') ) {
                 c = '_';
             }
            res.append(c);
        }
        while ( res.length() > 180 ) { // strange super long urls lead to "filename too long" errors. Just halve them
            StringBuilder newB = new StringBuilder(res.length()/2);
            for ( int i = 0; i < res.length(); i+=2 ) {
                newB.append( res.charAt(i) );
            }
            res = newB;
        }
        return res.toString();
    }

    public String toUrlString(boolean withProtocol) {
        String res = "";
        if ( protocol != null && withProtocol )
            res += protocol+"://";
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            res+=element;
            if ( i < elements.length-1 ) {
                res += "/";
            }
        }
        return res;
    }

    public KUrl getParentURL() {
        return concat("../");
    }

    public String getName() {
        if ( elements.length == 0 )
            return "";
        return elements[elements.length-1];
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof KUrl ) {
            KUrl other = (KUrl) obj;
            if ( other.getElements().length != elements.length )
                return false;
            if ( ! Objects.equals(other.getProtocol(),protocol) )
                return false;
            for (int i = 0; i < elements.length; i++) {
                String element = elements[i];
                if ( ! element.equals(other.getElements()[i]) ) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public boolean equalsIgnoreProtocol(Object obj) {
        if ( obj instanceof KUrl ) {
            KUrl other = (KUrl) obj;
            if ( other.getElements().length != elements.length )
                return false;
            for (int i = 0; i < elements.length; i++) {
                String element = elements[i];
                if ( ! element.equals(other.getElements()[i]) ) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hc = 0;
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            hc ^= element.hashCode();
        }
        if ( protocol != null ) {
            hc ^= protocol.hashCode();
        }
        return hc;
    }

    @Override
    public String toString() {
        return toUrlString();
    }

    public boolean isRelative() {
        return protocol == null;
    }

    public KUrl prepend(String name) {
        return new KUrl(name).concat(this);
    }

    public boolean startsWith(KUrl base) {
        String protocol = getProtocol().toLowerCase();
        if ( "https".equals(protocol) )
            protocol = "http";
        String baseProtocol = base.getProtocol();
        if ( "https".equals(baseProtocol) )
            baseProtocol = "http";
        if ( protocol == null ) {
            protocol = baseProtocol;
        }
        if ( base.getElements().length >= elements.length )
            return false;
        if ( !protocol.equals(baseProtocol) )
            return false; // FIXME: treat http and https equal ?
        for (int i = 0; i < base.getElements().length; i++) {
            if ( !elements[i].equalsIgnoreCase(base.getElements()[i]) ) {
                if ( i == 0 ) // hack: treat missing www. equal
                {
                    String a = normalizeDomain(elements[i]);
                    String b = normalizeDomain(base.getElements()[i]);
                    if ( a.equals(b) )
                    {
                        continue;
                    }
                }
                return false;
            }
        }
        return true;
    }

    /**
     * removes www, protocol and country
     * @return
     */
    public KUrl unified() {
        KUrl res = new KUrl(toUrlString(false));
        res.elements[0] = normalizeDomain(elements[0]);
        return res;
    }

    /**
     * removes 'www' in case and removes country code. EXPECT protocol to be absent
      * @param s
     * @return
     */
    protected String normalizeDomain(String s) {
        // remove country code
        int idx = s.lastIndexOf(".");
        if ( idx >= 0 ) {
            s = s.substring(0,idx);
        }
        if ( s.startsWith("www."))
            s = s.substring(4);
        return s;
    }

    public String getDomain() {
        return elements[0];
    }
}
