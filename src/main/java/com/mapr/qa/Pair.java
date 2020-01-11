package com.mapr.qa;

/**
 * A Pair <F,S> data structure
 *
 * @author the
 *
 * @param <F>
 *            first object type
 * @param <S>
 *            second object type
 */
public class Pair<F, S> {

    private static final int PRIME = 31;
    private F first;
    private S second;

    /**
     * Ctor
     */
    public Pair() {
    }

    /**
     * @param first
     *            first object in the pair.
     * @param second
     *            second object in the pair.
     */
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    /**
     * @return first object in the pair.
     */
    public F getFirst() {
        return first;
    }

    /**
     * @return second object in the pair.
     */
    public S getSecond() {
        return second;
    }

    /**
     * @param first
     *            the first to set
     */
    public void setFirst(F first) {
        this.first = first;
    }

    /**
     * @param second
     *            the second to set
     */
    public void setSecond(S second) {
        this.second = second;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {

        int result = 1;
        result = PRIME * result + ((first == null) ? 0 : first.hashCode());
        result = PRIME * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Pair<F, S> other = (Pair<F, S>) obj;
        if (first == null) {
            if (other.first != null) {
                return false;
            }
        } else if (!first.equals(other.first)) {
            return false;
        }
        if (second == null) {
            if (other.second != null) {
                return false;
            }
        } else if (!second.equals(other.second)) {
            return false;
        }
        return true;
    }

    public String toString() {
        return "[" + first + ", " + second + "]";
    }
}
